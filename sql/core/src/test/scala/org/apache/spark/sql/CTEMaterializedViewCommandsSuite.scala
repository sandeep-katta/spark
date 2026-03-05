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

import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.execution.ctemv.{
  CTEMVBackgroundWriter, CTEMVPersistentEntry, CTEMVPersistentRegistry, CTEMVRegistry}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for CTE-MV Phase 3: SQL management commands
 * (SHOW / REFRESH / DROP CTE MATERIALIZED VIEW[S]).
 */
class CTEMaterializedViewCommandsSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  // ── helpers ────────────────────────────────────────────────────────────────

  private def newPersistentRegistry(storePath: String) =
    new CTEMVPersistentRegistry(spark.sessionState.newHadoopConf(), storePath)

  private def withParquetMVWriter(
      registry: CTEMVRegistry,
      pr: CTEMVPersistentRegistry)(f: => Unit): Unit = {
    val writer = new CTEMVBackgroundWriter(spark, registry, Some(pr))
    spark.listenerManager.register(writer)
    try { f }
    finally { spark.listenerManager.unregister(writer) }
  }

  private def waitForMaterialization(pr: CTEMVPersistentRegistry): Unit = {
    spark.sparkContext.listenerBus.waitUntilEmpty()
    eventually(timeout(15.seconds), interval(300.milliseconds)) {
      assert(pr.allEntries().nonEmpty, "Expected at least one persistent entry")
    }
  }

  // ── SHOW CTE MATERIALIZED VIEWS ──────────────────────────────────────────

  test("SHOW CTE MATERIALIZED VIEWS returns empty when not in parquet mode") {
    withSQLConf(SQLConf.CTE_MV_ENABLED.key -> "true",
      SQLConf.CTE_MV_PERSISTENCE_MODE.key  -> "memory") {
      val rows = sql("SHOW CTE MATERIALIZED VIEWS").collect()
      assert(rows.isEmpty)
    }
  }

  test("SHOW CTE MATERIALIZED VIEWS returns empty when storePath not set") {
    withSQLConf(SQLConf.CTE_MV_ENABLED.key        -> "true",
      SQLConf.CTE_MV_PERSISTENCE_MODE.key          -> "parquet") {
      val rows = sql("SHOW CTE MATERIALIZED VIEWS").collect()
      assert(rows.isEmpty)
    }
  }

  test("SHOW CTE MATERIALIZED VIEWS returns registered entries") {
    withTempDir { dir =>
      val storePath = dir.getAbsolutePath
      val pr = newPersistentRegistry(storePath)
      pr.register(CTEMVPersistentEntry("fp0001", s"$storePath/d1", "__cte_mv_fp0001", 100L))
      pr.register(CTEMVPersistentEntry("fp0002", s"$storePath/d2", "__cte_mv_fp0002", 200L))

      withSQLConf(
        SQLConf.CTE_MV_ENABLED.key          -> "true",
        SQLConf.CTE_MV_PERSISTENCE_MODE.key -> "parquet",
        SQLConf.CTE_MV_STORE_PATH.key       -> storePath) {

        val rows = sql("SHOW CTE MATERIALIZED VIEWS").collect()
        assert(rows.length == 2)
        val fps = rows.map(_.getString(0)).toSet
        assert(fps == Set("fp0001", "fp0002"))
      }
    }
  }

  // ── REFRESH CTE MATERIALIZED VIEW ────────────────────────────────────────

  test("REFRESH CTE MATERIALIZED VIEW removes entry from registry") {
    withTempDir { dir =>
      val storePath = dir.getAbsolutePath
      val pr = newPersistentRegistry(storePath)
      pr.register(CTEMVPersistentEntry("fp_refresh_01", s"$storePath/d1",
        "__cte_mv_fp_refresh_01", 1L))

      withSQLConf(
        SQLConf.CTE_MV_ENABLED.key          -> "true",
        SQLConf.CTE_MV_PERSISTENCE_MODE.key -> "parquet",
        SQLConf.CTE_MV_STORE_PATH.key       -> storePath) {

        sql("REFRESH CTE MATERIALIZED VIEW '__cte_mv_fp_refresh_01'")

        // Entry should be removed.
        val fresh = newPersistentRegistry(storePath)
        assert(fresh.allEntries().isEmpty,
          "Registry should be empty after REFRESH")
      }
    }
  }

  test("REFRESH CTE MATERIALIZED VIEW with unknown name logs warning and does not throw") {
    withTempDir { dir =>
      withSQLConf(
        SQLConf.CTE_MV_ENABLED.key          -> "true",
        SQLConf.CTE_MV_PERSISTENCE_MODE.key -> "parquet",
        SQLConf.CTE_MV_STORE_PATH.key       -> dir.getAbsolutePath) {

        // Should not throw.
        sql("REFRESH CTE MATERIALIZED VIEW 'nonexistent_mv'")
      }
    }
  }

  // ── DROP CTE MATERIALIZED VIEW ────────────────────────────────────────────

  test("DROP CTE MATERIALIZED VIEW removes entry from registry") {
    withTempDir { dir =>
      val storePath = dir.getAbsolutePath
      val pr = newPersistentRegistry(storePath)
      pr.register(CTEMVPersistentEntry("fp_drop_01", s"$storePath/d1",
        "__cte_mv_fp_drop_01", 1L))

      withSQLConf(
        SQLConf.CTE_MV_ENABLED.key          -> "true",
        SQLConf.CTE_MV_PERSISTENCE_MODE.key -> "parquet",
        SQLConf.CTE_MV_STORE_PATH.key       -> storePath) {

        sql("DROP CTE MATERIALIZED VIEW '__cte_mv_fp_drop_01'")

        val fresh = newPersistentRegistry(storePath)
        assert(fresh.allEntries().isEmpty,
          "Registry should be empty after DROP")
      }
    }
  }

  test("DROP CTE MATERIALIZED VIEW IF EXISTS does not throw when entry is missing") {
    withTempDir { dir =>
      withSQLConf(
        SQLConf.CTE_MV_ENABLED.key          -> "true",
        SQLConf.CTE_MV_PERSISTENCE_MODE.key -> "parquet",
        SQLConf.CTE_MV_STORE_PATH.key       -> dir.getAbsolutePath) {

        // Should not throw.
        sql("DROP CTE MATERIALIZED VIEW IF EXISTS 'nonexistent_mv'")
      }
    }
  }

  test("DROP CTE MATERIALIZED VIEW without IF EXISTS throws on missing entry") {
    withTempDir { dir =>
      withSQLConf(
        SQLConf.CTE_MV_ENABLED.key          -> "true",
        SQLConf.CTE_MV_PERSISTENCE_MODE.key -> "parquet",
        SQLConf.CTE_MV_STORE_PATH.key       -> dir.getAbsolutePath) {

        val ex = intercept[Exception] {
          sql("DROP CTE MATERIALIZED VIEW 'nonexistent_mv'")
        }
        assert(ex.getMessage.contains("nonexistent_mv"))
      }
    }
  }

  // ── end-to-end: materialize then SHOW / DROP ──────────────────────────────

  test("end-to-end: materialize CTE, SHOW lists it, DROP removes it") {
    withTempDir { dir =>
      val storePath = dir.getAbsolutePath
      val registry  = new CTEMVRegistry()
      val pr        = newPersistentRegistry(storePath)

      withSQLConf(
        SQLConf.CTE_MV_ENABLED.key          -> "true",
        SQLConf.CTE_MV_PERSISTENCE_MODE.key -> "parquet",
        SQLConf.CTE_MV_STORE_PATH.key       -> storePath) {

        withParquetMVWriter(registry, pr) {
          withTempView("t1") {
            spark.range(10).createOrReplaceTempView("t1")
            sql("WITH cte AS (SELECT sum(id) s FROM t1) SELECT * FROM cte").collect()
            waitForMaterialization(pr)
          }
        }

        // SHOW should list the entry.
        val showRows = sql("SHOW CTE MATERIALIZED VIEWS").collect()
        assert(showRows.nonEmpty, "SHOW should return entries")

        val viewName = showRows.head.getString(0) // fingerprint
        // DROP by fingerprint.
        sql(s"DROP CTE MATERIALIZED VIEW '$viewName'")

        // SHOW should now be empty.
        val showAfter = sql("SHOW CTE MATERIALIZED VIEWS").collect()
        assert(showAfter.isEmpty, "SHOW should be empty after DROP")
      }
    }
  }
}
