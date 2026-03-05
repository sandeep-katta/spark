/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import org.apache.hadoop.conf.Configuration
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.execution.ctemv.{
  CTEFingerprint, CTEMVBackgroundWriter, CTEMVPersistentEntry, CTEMVPersistentRegistry,
  CTEMVRegistry, CTEMVRewriter}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for CTE-MV Phase 2: Parquet-backed persistent materialization.
 */
class CTEMaterializedViewPhase2Suite extends QueryTest with SharedSparkSession {

  import testImplicits._

  // ── helpers ────────────────────────────────────────────────────────────────

  private def newPersistentRegistry(storePath: String): CTEMVPersistentRegistry =
    new CTEMVPersistentRegistry(new Configuration(), storePath)

  private def withParquetMVWriter(
      registry: CTEMVRegistry,
      persistentRegistry: CTEMVPersistentRegistry)(f: => Unit): Unit = {
    val writer = new CTEMVBackgroundWriter(spark, registry, Some(persistentRegistry))
    spark.listenerManager.register(writer)
    try { f }
    finally { spark.listenerManager.unregister(writer) }
  }

  private def waitForMaterialization(
      pr: CTEMVPersistentRegistry,
      expectedCount: Int = 1): Unit = {
    spark.sparkContext.listenerBus.waitUntilEmpty()
    eventually(timeout(15.seconds), interval(300.milliseconds)) {
      assert(
        pr.allEntries().size >= expectedCount,
        s"Expected at least $expectedCount persistent entries but found ${pr.allEntries().size}")
    }
  }

  // ── tests ──────────────────────────────────────────────────────────────────

  test("eligible CTE body is written to Parquet and registered in persistent registry") {
    withTempDir { dir =>
      val storePath = dir.getAbsolutePath
      val registry   = new CTEMVRegistry()
      val pr         = newPersistentRegistry(storePath)

      withSQLConf(
        SQLConf.CTE_MV_ENABLED.key           -> "true",
        SQLConf.CTE_MV_PERSISTENCE_MODE.key  -> "parquet",
        SQLConf.CTE_MV_STORE_PATH.key        -> storePath) {

        withParquetMVWriter(registry, pr) {
          withTempView("t1") {
            spark.range(100).createOrReplaceTempView("t1")
            sql("WITH cte AS (SELECT sum(id) s FROM t1) SELECT * FROM cte").collect()
            waitForMaterialization(pr)

            val entries = pr.allEntries()
            assert(entries.nonEmpty, "Persistent registry should have an entry")
            assert(entries.head.parquetPath.startsWith(storePath),
              "Parquet path should be under storePath")
          }
        }
      }
    }
  }

  test("Parquet MV entry survives across simulated session restart (new registry instance)") {
    withTempDir { dir =>
      val storePath = dir.getAbsolutePath
      val registry   = new CTEMVRegistry()
      val pr         = newPersistentRegistry(storePath)

      withSQLConf(
        SQLConf.CTE_MV_ENABLED.key           -> "true",
        SQLConf.CTE_MV_PERSISTENCE_MODE.key  -> "parquet",
        SQLConf.CTE_MV_STORE_PATH.key        -> storePath) {

        withParquetMVWriter(registry, pr) {
          withTempView("t1") {
            spark.range(50).createOrReplaceTempView("t1")
            sql("WITH cte AS (SELECT sum(id) s FROM t1) SELECT * FROM cte").collect()
            waitForMaterialization(pr)
          }
        }
      }

      // Simulate a new session: create a fresh registry pointing at the same storePath.
      val newPr = newPersistentRegistry(storePath)
      val entries = newPr.allEntries()
      assert(entries.nonEmpty,
        "Registry should persist across simulated session restart")
      assert(entries.head.parquetPath.nonEmpty)
    }
  }

  test("CTEMVRewriter substitutes CTERelationRef with LogicalRelation from Parquet") {
    withTempDir { dir =>
      val storePath = dir.getAbsolutePath
      val registry   = new CTEMVRegistry()
      val pr         = newPersistentRegistry(storePath)

      withSQLConf(
        SQLConf.CTE_MV_ENABLED.key           -> "true",
        SQLConf.CTE_MV_PERSISTENCE_MODE.key  -> "parquet",
        SQLConf.CTE_MV_STORE_PATH.key        -> storePath) {

        withParquetMVWriter(registry, pr) {
          withTempView("t1") {
            spark.range(100).createOrReplaceTempView("t1")
            // First run: materializes the CTE body to Parquet.
            sql("WITH cte AS (SELECT sum(id) s FROM t1) SELECT * FROM cte").collect()
            waitForMaterialization(pr)

            // Directly invoke CTEMVRewriter to verify substitution.
            val rewriter = new CTEMVRewriter(spark, pr)
            val query    = "WITH cte AS (SELECT sum(id) s FROM t1) SELECT * FROM cte"
            val analyzed = spark.sql(query).queryExecution.analyzed
            val rewritten = rewriter.apply(analyzed)

            // The rewritten plan should contain a LogicalRelation (Parquet scan).
            assert(
              rewritten.exists(_.isInstanceOf[LogicalRelation]),
              "Rewritten plan should contain a LogicalRelation (Parquet scan)")
          }
        }
      }
    }
  }

  test("expired TTL entries are NOT substituted by CTEMVRewriter") {
    withTempDir { dir =>
      val storePath = dir.getAbsolutePath
      val pr = newPersistentRegistry(storePath)

      // Manually insert a stale entry (createdAt = 0 → always expired).
      pr.register(CTEMVPersistentEntry(
        fingerprint    = "stale_fp_0000000000000000",
        parquetPath    = s"$storePath/__cte_mv_stale",
        globalViewName = "__cte_mv_stale",
        createdAt      = 0L))

      withSQLConf(
        SQLConf.CTE_MV_ENABLED.key           -> "true",
        SQLConf.CTE_MV_PERSISTENCE_MODE.key  -> "parquet",
        SQLConf.CTE_MV_STORE_PATH.key        -> storePath,
        SQLConf.CTE_MV_TTL_SECONDS.key       -> "1") {

        withTempView("t1") {
          spark.range(10).createOrReplaceTempView("t1")
          val rewriter = new CTEMVRewriter(spark, pr)
          val analyzed = spark.sql(
            "WITH cte AS (SELECT sum(id) s FROM t1) SELECT * FROM cte"
          ).queryExecution.analyzed
          val rewritten = rewriter.apply(analyzed)

          // No substitution should happen because the entry is expired.
          assert(
            !rewritten.exists(_.isInstanceOf[LogicalRelation]),
            "Expired entry should not be substituted")
        }
      }
    }
  }

  test("non-deterministic CTE is not persisted to Parquet") {
    withTempDir { dir =>
      val storePath = dir.getAbsolutePath
      val registry   = new CTEMVRegistry()
      val pr         = newPersistentRegistry(storePath)

      withSQLConf(
        SQLConf.CTE_MV_ENABLED.key           -> "true",
        SQLConf.CTE_MV_PERSISTENCE_MODE.key  -> "parquet",
        SQLConf.CTE_MV_STORE_PATH.key        -> storePath) {

        withParquetMVWriter(registry, pr) {
          withTempView("t1") {
            spark.range(10).createOrReplaceTempView("t1")
            sql("WITH cte AS (SELECT id, rand() r FROM t1) SELECT * FROM cte").collect()
            spark.sparkContext.listenerBus.waitUntilEmpty()
            Thread.sleep(500)
            assert(pr.allEntries().isEmpty,
              "Non-deterministic CTE should not be persisted to Parquet")
          }
        }
      }
    }
  }

  test("CTEMVPersistentRegistry round-trips entries correctly") {
    withTempDir { dir =>
      val pr = newPersistentRegistry(dir.getAbsolutePath)
      val entry = CTEMVPersistentEntry("fp001234", "/some/path", "__cte_mv_fp001234", 1234567890L)
      pr.register(entry)

      val readBack = pr.lookup("fp001234")
      assert(readBack.isDefined)
      assert(readBack.get == entry)

      // Second registration replaces the first (no duplicate).
      val entry2 = entry.copy(createdAt = 9999999999L)
      pr.register(entry2)
      assert(pr.allEntries().size == 1)
      assert(pr.allEntries().head.createdAt == 9999999999L)
    }
  }
}
