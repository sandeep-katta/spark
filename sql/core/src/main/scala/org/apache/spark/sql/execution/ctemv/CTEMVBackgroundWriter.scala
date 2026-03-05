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

package org.apache.spark.sql.execution.ctemv

import java.util.concurrent.ExecutorService

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationDef, CTERelationRef, LogicalPlan, WithCTE}
import org.apache.spark.sql.classic.{Dataset, SparkSession}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.ThreadUtils

/**
 * A [[QueryExecutionListener]] that asynchronously materializes eligible CTE bodies
 * after a query succeeds.
 *
 * **Mode "memory" (Phase 1):**
 * The CTE body is persisted with `StorageLevel.MEMORY_AND_DISK` and registered as a local
 * temp view. Spark's `CacheManager.useCachedData()` then substitutes subsequent identical
 * CTE bodies with an `InMemoryRelation` automatically.
 *
 * **Mode "parquet" (Phase 2):**
 * The CTE body is written to Parquet under `spark.sql.cte.mv.storePath/<fp16>/` and the
 * resulting path is recorded in the [[CTEMVPersistentRegistry]]. On subsequent queries
 * [[CTEMVRewriter]] (an optimizer rule) reads the registry and substitutes `CTERelationRef`
 * nodes with Parquet scans — enabling cross-session reuse.
 *
 * Eligibility:
 *  - Deterministic (no `rand()`, `now()`, etc.)
 *  - Not recursive (`!hasSelfReferenceAsUnionLoopRef`)
 *  - No cross-CTE references in child (`!child.exists(_.isInstanceOf[CTERelationRef])`)
 *  - Fingerprint not already registered
 */
class CTEMVBackgroundWriter(
    session: SparkSession,
    registry: CTEMVRegistry,
    persistentRegistry: Option[CTEMVPersistentRegistry] = None)
  extends QueryExecutionListener with Logging {

  private val pool: ExecutorService =
    ThreadUtils.newDaemonCachedThreadPool("cte-mv-writer", 2)

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    val sparkSession = qe.sparkSession
    val conf = sparkSession.sessionState.conf
    if (!conf.getConf(SQLConf.CTE_MV_ENABLED)) return

    extractEligibleCTEDefs(qe.analyzed)
      .filter(d => !registry.contains(CTEFingerprint.compute(d.child)))
      .foreach { d =>
        pool.submit(new Runnable {
          override def run(): Unit = {
            conf.getConf(SQLConf.CTE_MV_PERSISTENCE_MODE) match {
              case "parquet" => materializeParquet(sparkSession, d, conf)
              case _         => materializeMemory(sparkSession, d, conf)
            }
          }
        })
      }
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}

  // ── eligibility ─────────────────────────────────────────────────────────────

  private[ctemv] def extractEligibleCTEDefs(plan: LogicalPlan): Seq[CTERelationDef] = {
    plan.collect { case WithCTE(_, defs) => defs }.flatten
      .filter { d =>
        d.deterministic &&
          !d.hasSelfReferenceAsUnionLoopRef &&
          !d.child.exists(_.isInstanceOf[CTERelationRef])
      }
  }

  // ── Phase 1: in-memory cache ─────────────────────────────────────────────────

  private def materializeMemory(
      sparkSession: SparkSession,
      cteDef: CTERelationDef,
      conf: org.apache.spark.sql.internal.SQLConf): Unit = {
    val fp = CTEFingerprint.compute(cteDef.child)
    if (registry.contains(fp)) return
    try {
      val prefix   = conf.getConf(SQLConf.CTE_MV_VIEW_PREFIX)
      val viewName = s"${prefix}_${fp.take(16)}"
      val df       = Dataset.ofRows(sparkSession, cteDef.child)
      df.persist(StorageLevel.MEMORY_AND_DISK)
      df.count()                              // trigger execution, fill the cache
      df.createOrReplaceTempView(viewName)
      registry.register(fp, CTEMVEntry(fp, viewName, System.currentTimeMillis()))
      logInfo(s"CTE-MV[memory]: cached CTE body as '$viewName' (fp=$fp)")
    } catch {
      case NonFatal(e) =>
        logWarning(s"CTE-MV[memory]: failed to materialize (fp=$fp): ${e.getMessage}", e)
    }
  }

  // ── Phase 2: Parquet-backed ──────────────────────────────────────────────────

  private def materializeParquet(
      sparkSession: SparkSession,
      cteDef: CTERelationDef,
      conf: org.apache.spark.sql.internal.SQLConf): Unit = {
    val fp = CTEFingerprint.compute(cteDef.child)
    if (registry.contains(fp)) return
    try {
      val storePath = conf.getConf(SQLConf.CTE_MV_STORE_PATH).getOrElse {
        logWarning("CTE-MV[parquet]: spark.sql.cte.mv.storePath is not set; skipping.")
        return
      }
      val prefix      = conf.getConf(SQLConf.CTE_MV_VIEW_PREFIX)
      val fp16        = fp.take(16)
      val parquetPath = s"$storePath/${prefix}_$fp16"
      val viewName    = s"${prefix}_$fp16"

      // ① Write to Parquet (idempotent: overwrite mode).
      val df = Dataset.ofRows(sparkSession, cteDef.child)
      df.write.mode("overwrite").parquet(parquetPath)
      logInfo(s"CTE-MV[parquet]: wrote CTE body to $parquetPath (fp=$fp)")

      // ② Register in persistent registry (cross-session).
      persistentRegistry.foreach { pr =>
        pr.register(CTEMVPersistentEntry(fp, parquetPath, viewName,
          System.currentTimeMillis()))
      }

      // ③ Also update in-memory registry so CTEMVRewriter sees it in this session
      //    without waiting for the next optimizer run.
      registry.register(fp, CTEMVEntry(fp, viewName, System.currentTimeMillis()))

      logInfo(s"CTE-MV[parquet]: registered '$viewName' (fp=$fp)")
    } catch {
      case NonFatal(e) =>
        logWarning(s"CTE-MV[parquet]: failed to materialize (fp=$fp): ${e.getMessage}", e)
    }
  }
}
