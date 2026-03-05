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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LeafRunnableCommand
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{LongType, StringType}

/**
 * `SHOW CTE MATERIALIZED VIEWS`
 *
 * Returns one row per live persistent MV entry in the configured store path.
 * Columns: fingerprint STRING, parquet_path STRING, created_at BIGINT.
 */
case object ShowCteMvsCommand extends LeafRunnableCommand with Logging {

  override val output: Seq[Attribute] = Seq(
    AttributeReference("fingerprint",  StringType, nullable = false)(),
    AttributeReference("parquet_path", StringType, nullable = false)(),
    AttributeReference("created_at",   LongType,   nullable = false)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val conf = sparkSession.sessionState.conf
    if (!conf.getConf(SQLConf.CTE_MV_ENABLED)) return Seq.empty
    if (conf.getConf(SQLConf.CTE_MV_PERSISTENCE_MODE) != "parquet") return Seq.empty

    conf.getConf(SQLConf.CTE_MV_STORE_PATH) match {
      case None =>
        logWarning("SHOW CTE MATERIALIZED VIEWS: spark.sql.cte.mv.storePath is not set.")
        Seq.empty
      case Some(storePath) =>
        val pr = new CTEMVPersistentRegistry(
          sparkSession.sessionState.newHadoopConf(), storePath)
        pr.allEntries().map { e =>
          Row(e.fingerprint, e.parquetPath, e.createdAt)
        }
    }
  }
}

/**
 * `REFRESH CTE MATERIALIZED VIEW <name>`
 *
 * Deletes the Parquet data and removes the registry entry for the MV identified
 * by its fingerprint (short-name, e.g. `__cte_mv_<fp16>`). The next query that
 * matches this CTE will re-materialize from scratch.
 */
case class RefreshCteMvCommand(name: String) extends LeafRunnableCommand with Logging {

  override val output: Seq[Attribute] = Seq.empty

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val conf = sparkSession.sessionState.conf
    if (!conf.getConf(SQLConf.CTE_MV_ENABLED)) return Seq.empty

    conf.getConf(SQLConf.CTE_MV_STORE_PATH) match {
      case None =>
        logWarning(s"REFRESH CTE MATERIALIZED VIEW $name: " +
          "spark.sql.cte.mv.storePath is not set.")
      case Some(storePath) =>
        val hadoopConf = sparkSession.sessionState.newHadoopConf()
        val pr = new CTEMVPersistentRegistry(hadoopConf, storePath)
        pr.allEntries().find(e => e.globalViewName == name || e.fingerprint == name) match {
          case None =>
            logWarning(s"REFRESH CTE MATERIALIZED VIEW: no entry found for '$name'")
          case Some(entry) =>
            // Delete Parquet data.
            deleteParquetData(entry.parquetPath, hadoopConf)
            // Remove from persistent registry.
            pr.remove(entry.fingerprint)
            logInfo(s"CTE-MV: refreshed '$name' (fp=${entry.fingerprint})")
        }
    }
    Seq.empty
  }

  private def deleteParquetData(parquetPath: String, hadoopConf: Configuration): Unit = {
    try {
      val path = new Path(parquetPath)
      val fs   = FileSystem.get(path.toUri, hadoopConf)
      if (fs.exists(path)) fs.delete(path, /* recursive */ true)
    } catch {
      case scala.util.control.NonFatal(e) =>
        logWarning(s"CTE-MV: could not delete Parquet data at $parquetPath: ${e.getMessage}")
    }
  }
}

/**
 * `DROP CTE MATERIALIZED VIEW [IF EXISTS] <name>`
 *
 * Same as REFRESH but additionally logs at INFO level to distinguish the intent:
 * "permanently remove" vs "invalidate for re-computation". Both operations are
 * functionally identical (delete data + deregister).
 */
case class DropCteMvCommand(name: String, ifExists: Boolean)
  extends LeafRunnableCommand with Logging {

  override val output: Seq[Attribute] = Seq.empty

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val conf = sparkSession.sessionState.conf
    if (!conf.getConf(SQLConf.CTE_MV_ENABLED)) return Seq.empty

    conf.getConf(SQLConf.CTE_MV_STORE_PATH) match {
      case None =>
        logWarning(s"DROP CTE MATERIALIZED VIEW $name: " +
          "spark.sql.cte.mv.storePath is not set.")
      case Some(storePath) =>
        val hadoopConf = sparkSession.sessionState.newHadoopConf()
        val pr = new CTEMVPersistentRegistry(hadoopConf, storePath)
        pr.allEntries().find(e => e.globalViewName == name || e.fingerprint == name) match {
          case None if ifExists =>
            logInfo(s"DROP CTE MATERIALIZED VIEW IF EXISTS: no entry for '$name'; skipping.")
          case None =>
            throw new AnalysisException(
              errorClass = "TABLE_OR_VIEW_NOT_FOUND",
              messageParameters = Map("relationName" -> s"`$name`"))
          case Some(entry) =>
            deleteParquetData(entry.parquetPath, hadoopConf)
            pr.remove(entry.fingerprint)
            logInfo(s"CTE-MV: dropped '$name' (fp=${entry.fingerprint})")
        }
    }
    Seq.empty
  }

  private def deleteParquetData(parquetPath: String, hadoopConf: Configuration): Unit = {
    try {
      val path = new Path(parquetPath)
      val fs   = FileSystem.get(path.toUri, hadoopConf)
      if (fs.exists(path)) fs.delete(path, /* recursive */ true)
    } catch {
      case scala.util.control.NonFatal(e) =>
        logWarning(s"CTE-MV: could not delete Parquet data at $parquetPath: ${e.getMessage}")
    }
  }
}
