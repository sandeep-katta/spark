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

import java.net.URI

import scala.util.control.NonFatal

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationDef, CTERelationRef, LogicalPlan,
  Project, WithCTE}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{CTE, PLAN_EXPRESSION}
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.internal.SQLConf

/**
 * Optimizer rule (Phase 2) that substitutes eligible `CTERelationRef` nodes with
 * Parquet-backed scans looked up from [[CTEMVPersistentRegistry]].
 *
 * This rule runs in `preOptimizationBatches` — before the "Inline CTE" batch — so that
 * replaced definitions are dropped with zero ref-count by the subsequent InlineCTE pass.
 *
 * Eligibility (same as [[CTEMVBackgroundWriter]]):
 *  - Parquet persistence mode is active (`spark.sql.cte.mv.persistenceMode == "parquet"`).
 *  - The fingerprint is present in the persistent registry.
 *  - The entry has not exceeded TTL (`spark.sql.cte.mv.ttlSeconds`).
 *  - The Parquet path is accessible on the filesystem.
 */
class CTEMVRewriter(session: SparkSession, registry: CTEMVPersistentRegistry)
  extends Rule[LogicalPlan] with Logging {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val conf = session.sessionState.conf
    if (!conf.getConf(SQLConf.CTE_MV_ENABLED)) return plan
    if (conf.getConf(SQLConf.CTE_MV_PERSISTENCE_MODE) != "parquet") return plan
    if (!plan.containsPattern(CTE)) return plan
    rewritePlan(plan, conf.getConf(SQLConf.CTE_MV_TTL_SECONDS))
  }

  // ── plan traversal ─────────────────────────────────────────────────────────

  private def rewritePlan(plan: LogicalPlan, ttlSeconds: Long): LogicalPlan = plan match {
    case WithCTE(child, defs) =>
      val idToScan = buildScanMap(defs, ttlSeconds)
      if (idToScan.isEmpty) return plan

      val newChild  = replaceRefs(child, idToScan)
      val newDefs   = defs
        .filterNot(d => idToScan.contains(d.id))         // drop rewritten defs
        .map(d => d.copy(child = replaceRefs(d.child, idToScan)))  // update cross-def refs

      if (newDefs.isEmpty) newChild else WithCTE(newChild, newDefs)

    case _ if plan.containsPattern(CTE) =>
      plan
        .withNewChildren(plan.children.map(rewritePlan(_, ttlSeconds)))
        .transformExpressionsWithPruning(_.containsAllPatterns(PLAN_EXPRESSION, CTE)) {
          case e: SubqueryExpression => e.withNewPlan(rewritePlan(e.plan, ttlSeconds))
        }

    case _ => plan
  }

  /**
   * For every `CTERelationDef` in `defs` that has a live Parquet MV, build a map
   * `def.id -> analyzed LogicalPlan` (a `LogicalRelation` from the Parquet files).
   */
  private def buildScanMap(
      defs: Seq[CTERelationDef],
      ttlSeconds: Long): Map[Long, LogicalPlan] = {
    defs.flatMap { d =>
      val fp = CTEFingerprint.compute(d.child)
      registry.lookup(fp) match {
        case Some(entry) if !isExpired(entry, ttlSeconds) && parquetExists(entry.parquetPath) =>
          buildScan(entry.parquetPath).map(scan => d.id -> scan)
        case _ => None
      }
    }.toMap
  }

  /**
   * Constructs an analyzed `LogicalPlan` (a `LogicalRelation`) pointing at the given
   * Parquet path, using the v1 DataSource API. This does NOT trigger a new QueryExecution;
   * it only reads the Parquet footer for schema information.
   */
  private def buildScan(parquetPath: String): Option[LogicalPlan] = {
    try {
      val baseRelation = DataSource(
        sparkSession  = session,
        className     = "parquet",
        paths         = Seq(parquetPath)
      ).resolveRelation()
      Some(LogicalRelation(baseRelation))
    } catch {
      case NonFatal(e) =>
        logWarning(
          s"CTE-MV: failed to create scan for Parquet MV at $parquetPath: ${e.getMessage}")
        None
    }
  }

  // ── reference replacement ──────────────────────────────────────────────────

  /**
   * Replaces `CTERelationRef` nodes in `plan` with their corresponding scans.
   * Does NOT recurse into nested `WithCTE` nodes — those are handled by the outer
   * `rewritePlan` traversal.
   */
  private def replaceRefs(
      plan: LogicalPlan,
      idToScan: Map[Long, LogicalPlan]): LogicalPlan = {
    plan.transformDown {
      case w: WithCTE => w                  // leave nested WithCTE for outer rewritePlan pass

      case ref: CTERelationRef if idToScan.contains(ref.cteId) =>
        alignAttributes(ref.output, idToScan(ref.cteId))
    }
  }

  /**
   * Projects the scan's output to match `expectedOutput`, remapping ExprIds so that
   * downstream references to CTERelationRef attributes continue to resolve correctly.
   * Mirrors the `InlineCTE` approach.
   */
  private def alignAttributes(
      expectedOutput: Seq[Attribute],
      scan: LogicalPlan): LogicalPlan = {
    if (expectedOutput.size != scan.output.size) {
      // Schema mismatch — skip substitution for safety.
      logWarning("CTE-MV: attribute count mismatch between CTE ref and Parquet scan; skipping.")
      scan
    } else {
      val projectList = expectedOutput.zip(scan.output).map { case (tgt, src) =>
        if (src.semanticEquals(tgt)) tgt
        else Alias(src, tgt.name)(exprId = tgt.exprId)
      }
      if (projectList.forall(_.isInstanceOf[Attribute])) scan
      else Project(projectList, scan)
    }
  }

  // ── helpers ────────────────────────────────────────────────────────────────

  private def isExpired(entry: CTEMVPersistentEntry, ttlSeconds: Long): Boolean =
    (System.currentTimeMillis() - entry.createdAt) >= ttlSeconds * 1000L

  private def parquetExists(parquetPath: String): Boolean =
    try {
      val fs = FileSystem.get(new URI(parquetPath), session.sessionState.newHadoopConf())
      fs.exists(new Path(parquetPath))
    } catch {
      case NonFatal(_) => false
    }
}
