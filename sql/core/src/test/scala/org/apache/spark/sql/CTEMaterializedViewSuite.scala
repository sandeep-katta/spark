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

import org.apache.spark.sql.execution.ctemv.{CTEMVBackgroundWriter, CTEMVRegistry}
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class CTEMaterializedViewSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  /**
   * Registers a [[CTEMVBackgroundWriter]] for the duration of the test body, then removes it.
   * The caller is responsible for setting [[SQLConf.CTE_MV_ENABLED]] to true via `withSQLConf`.
   */
  private def withCTEMVWriter(registry: CTEMVRegistry)(f: => Unit): Unit = {
    val writer = new CTEMVBackgroundWriter(spark, registry)
    spark.listenerManager.register(writer)
    try { f }
    finally { spark.listenerManager.unregister(writer) }
  }

  /** Waits for the listener bus and background threads to quiesce (up to `timeoutMs`). */
  private def waitForMaterialization(registry: CTEMVRegistry, expectedCount: Int = 1): Unit = {
    spark.sparkContext.listenerBus.waitUntilEmpty()
    eventually(timeout(10.seconds), interval(200.milliseconds)) {
      assert(
        registry.entries().size >= expectedCount,
        s"Expected at least $expectedCount registry entries but got ${registry.entries().size}")
    }
  }

  test("eligible CTE body is materialized into registry after first execution") {
    val registry = new CTEMVRegistry()
    withSQLConf(SQLConf.CTE_MV_ENABLED.key -> "true") {
      withCTEMVWriter(registry) {
        withTempView("t1") {
          spark.range(100).createOrReplaceTempView("t1")
          sql("WITH cte AS (SELECT sum(id) s FROM t1) SELECT * FROM cte").collect()
          waitForMaterialization(registry)
          assert(registry.entries().nonEmpty, "Registry should contain the CTE fingerprint")
        }
      }
    }
  }

  test("second execution with same CTE structure uses InMemoryRelation") {
    val registry = new CTEMVRegistry()
    withSQLConf(SQLConf.CTE_MV_ENABLED.key -> "true") {
      withCTEMVWriter(registry) {
        withTempView("t1") {
          spark.range(100).createOrReplaceTempView("t1")
          // First execution: triggers materialization in the background.
          sql("WITH cte AS (SELECT sum(id) s FROM t1) SELECT * FROM cte").collect()
          waitForMaterialization(registry)

          // Second execution: CacheManager should substitute the CTE body with InMemoryRelation.
          val df = sql("WITH cte AS (SELECT sum(id) s FROM t1) SELECT * FROM cte")
          val hasInMemoryRelation = df.queryExecution.optimizedPlan.exists {
            case _: InMemoryRelation => true
            case _ => false
          }
          assert(hasInMemoryRelation,
            "Optimized plan should contain InMemoryRelation after CTE body is cached")
        }
      }
    }
  }

  test("non-deterministic CTE (rand()) is NOT materialized") {
    val registry = new CTEMVRegistry()
    withSQLConf(SQLConf.CTE_MV_ENABLED.key -> "true") {
      withCTEMVWriter(registry) {
        withTempView("t1") {
          spark.range(100).createOrReplaceTempView("t1")
          sql(
            """WITH cte AS (SELECT id, rand() r FROM t1)
              |SELECT * FROM cte""".stripMargin).collect()
          // Give any potential background work time to complete.
          spark.sparkContext.listenerBus.waitUntilEmpty()
          Thread.sleep(500)
          assert(registry.entries().isEmpty,
            "Non-deterministic CTE should not be materialized")
        }
      }
    }
  }

  test("recursive CTE is NOT materialized") {
    val registry = new CTEMVRegistry()
    withSQLConf(SQLConf.CTE_MV_ENABLED.key -> "true") {
      withCTEMVWriter(registry) {
        // Recursive CTEs contain UnionLoopRef, making hasSelfReferenceAsUnionLoopRef = true.
        sql(
          """WITH RECURSIVE cte(n) AS (
            |  SELECT 1
            |  UNION ALL
            |  SELECT n + 1 FROM cte WHERE n < 5
            |)
            |SELECT * FROM cte""".stripMargin).collect()
        spark.sparkContext.listenerBus.waitUntilEmpty()
        Thread.sleep(500)
        assert(registry.entries().isEmpty, "Recursive CTE should not be materialized")
      }
    }
  }

  test("CTE_MV_ENABLED = false disables materialization entirely") {
    val registry = new CTEMVRegistry()
    withSQLConf(SQLConf.CTE_MV_ENABLED.key -> "false") {
      withCTEMVWriter(registry) {
        withTempView("t1") {
          spark.range(100).createOrReplaceTempView("t1")
          sql("WITH cte AS (SELECT sum(id) s FROM t1) SELECT * FROM cte").collect()
          spark.sparkContext.listenerBus.waitUntilEmpty()
          Thread.sleep(500)
          assert(registry.entries().isEmpty,
            "No materialization should occur when CTE_MV_ENABLED is false")
        }
      }
    }
  }
}
