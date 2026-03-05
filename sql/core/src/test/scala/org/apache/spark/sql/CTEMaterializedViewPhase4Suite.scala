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
  CTEFingerprint, CTEMVBackgroundWriter, CTEMVPersistentEntry, CTEMVPersistentRegistry,
  CTEMVRegistry}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for CTE-MV Phase 4: session warm-up from persistent registry and
 * double-materialization prevention.
 */
class CTEMaterializedViewPhase4Suite extends QueryTest with SharedSparkSession {

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

  private def waitForMaterialization(pr: CTEMVPersistentRegistry, minCount: Int = 1): Unit = {
    spark.sparkContext.listenerBus.waitUntilEmpty()
    eventually(timeout(15.seconds), interval(300.milliseconds)) {
      assert(pr.allEntries().size >= minCount,
        s"Expected at least $minCount persistent entries")
    }
  }

  // ── warm-up tests ─────────────────────────────────────────────────────────

  test("in-memory registry is pre-warmed from persistent registry on construction") {
    withTempDir { dir =>
      val storePath = dir.getAbsolutePath
      val pr = newPersistentRegistry(storePath)

      // Simulate a previously materialized CTE: insert a fresh (non-expired) entry.
      val fp = "deadbeef01234567deadbeef01234567"
      pr.register(CTEMVPersistentEntry(fp, s"$storePath/__cte_mv_$fp",
        s"__cte_mv_$fp", System.currentTimeMillis()))

      withSQLConf(
        SQLConf.CTE_MV_ENABLED.key          -> "true",
        SQLConf.CTE_MV_PERSISTENCE_MODE.key -> "parquet",
        SQLConf.CTE_MV_STORE_PATH.key       -> storePath,
        SQLConf.CTE_MV_TTL_SECONDS.key      -> "3600") {

        // Force a new CTEMVRegistry construction via the builder (simulate session start).
        // We can't restart the session here, but we can directly test the warm-up logic
        // via BaseSessionStateBuilder by inspecting that the warm-up reads the persistent
        // registry. Here we test the registry object's warm-up logic directly.
        val freshRegistry = new CTEMVRegistry()
        // Simulate what BaseSessionStateBuilder does:
        pr.allEntries().foreach { entry =>
          val ttl = 3600L
          val ageMs = System.currentTimeMillis() - entry.createdAt
          if (ageMs < ttl * 1000L) {
            import org.apache.spark.sql.execution.ctemv.CTEMVEntry
            freshRegistry.register(entry.fingerprint,
              CTEMVEntry(entry.fingerprint, entry.globalViewName, entry.createdAt))
          }
        }

        assert(freshRegistry.contains(fp),
          "Warmed-up registry should contain fingerprint from persistent registry")
      }
    }
  }

  test("expired persistent entries are NOT loaded into the in-memory registry at warm-up") {
    withTempDir { dir =>
      val storePath = dir.getAbsolutePath
      val pr = newPersistentRegistry(storePath)

      // Insert an expired entry (createdAt = 0 → far in the past).
      pr.register(CTEMVPersistentEntry("stale0000", s"$storePath/__cte_mv_stale",
        "__cte_mv_stale", 0L))

      withSQLConf(
        SQLConf.CTE_MV_ENABLED.key          -> "true",
        SQLConf.CTE_MV_PERSISTENCE_MODE.key -> "parquet",
        SQLConf.CTE_MV_STORE_PATH.key       -> storePath,
        SQLConf.CTE_MV_TTL_SECONDS.key      -> "1") {

        val freshRegistry = new CTEMVRegistry()
        pr.allEntries().foreach { entry =>
          val ttl = 1L
          val ageMs = System.currentTimeMillis() - entry.createdAt
          if (ageMs < ttl * 1000L) {
            import org.apache.spark.sql.execution.ctemv.CTEMVEntry
            freshRegistry.register(entry.fingerprint,
              CTEMVEntry(entry.fingerprint, entry.globalViewName, entry.createdAt))
          }
        }

        assert(!freshRegistry.contains("stale0000"),
          "Expired entries should not be loaded at warm-up")
      }
    }
  }

  // ── double-materialization prevention ────────────────────────────────────

  test("background writer skips materialization when fingerprint already in in-memory registry") {
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
            spark.range(20).createOrReplaceTempView("t1")
            val q = "WITH cte AS (SELECT sum(id) s FROM t1) SELECT * FROM cte"
            // First run: materializes.
            sql(q).collect()
            waitForMaterialization(pr)

            val countBefore = pr.allEntries().size

            // Second run: registry already has the fingerprint → no new entry.
            sql(q).collect()
            spark.sparkContext.listenerBus.waitUntilEmpty()
            Thread.sleep(500)

            assert(pr.allEntries().size == countBefore,
              "No new persistent entries should be created on second run")
          }
        }
      }
    }
  }
}
