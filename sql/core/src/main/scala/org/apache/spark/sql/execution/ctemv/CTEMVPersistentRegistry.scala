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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.Logging

/**
 * Metadata entry stored in the cross-session persistent registry.
 *
 * @param fingerprint   Stable SHA-256 fingerprint of the CTE body plan (first 32 hex chars).
 * @param parquetPath   Absolute filesystem path to the materialized Parquet dataset.
 * @param globalViewName Name of the global temp view registered in the catalog for this MV.
 * @param createdAt     Epoch milliseconds when this entry was written.
 */
case class CTEMVPersistentEntry(
    fingerprint: String,
    parquetPath: String,
    globalViewName: String,
    createdAt: Long)

/**
 * Cross-session, file-backed registry for Parquet-persisted CTE materialized views.
 *
 * Entries are stored as TSV lines (`fingerprint TAB parquetPath TAB globalViewName TAB createdAt`)
 * in a file called `_ctemv_registry.tsv` under `storePath`. Writes are atomic: the file is
 * written to a temporary path and renamed into place.
 *
 * Multiple sessions sharing the same `storePath` will see each other's entries on the next
 * registry read (reads are always fresh from disk).
 *
 * Thread-safety within a single JVM is provided by `synchronized` on write operations.
 */
class CTEMVPersistentRegistry(hadoopConf: Configuration, storePath: String)
  extends Logging {

  private val registryFile = new Path(storePath, "_ctemv_registry.tsv")
  private val tmpFile      = new Path(storePath, "_ctemv_registry.tsv.tmp")

  /** Returns the entry for `fp`, or `None` if not found or the file is unreadable. */
  def lookup(fp: String): Option[CTEMVPersistentEntry] = allEntries().find(_.fingerprint == fp)

  def contains(fp: String): Boolean = lookup(fp).isDefined

  /**
   * Appends (or replaces) an entry for `fp`. Any existing entry with the same fingerprint
   * is removed first to avoid duplicates.
   */
  def register(entry: CTEMVPersistentEntry): Unit = synchronized {
    val existing = allEntries().filterNot(_.fingerprint == entry.fingerprint)
    writeEntries(existing :+ entry)
  }

  /**
   * Removes the entry with the given fingerprint, if present.
   * No-op if the fingerprint is not found.
   */
  def remove(fp: String): Unit = synchronized {
    val remaining = allEntries().filterNot(_.fingerprint == fp)
    writeEntries(remaining)
  }

  /** Returns all entries currently persisted (reads from disk every time). */
  def allEntries(): Seq[CTEMVPersistentEntry] = {
    try {
      val fs = fileSystem()
      if (!fs.exists(registryFile)) return Seq.empty
      val in  = fs.open(registryFile)
      val raw = scala.io.Source.fromInputStream(in, "UTF-8").mkString
      in.close()
      raw.linesIterator
        .filter(_.nonEmpty)
        .flatMap(parseLine)
        .toSeq
    } catch {
      case NonFatal(e) =>
        logWarning(s"CTE-MV: failed to read persistent registry at $registryFile: $e")
        Seq.empty
    }
  }

  // ── private helpers ────────────────────────────────────────────────────────

  private def fileSystem(): FileSystem =
    FileSystem.get(new URI(storePath), hadoopConf)

  private def writeEntries(entries: Seq[CTEMVPersistentEntry]): Unit = {
    try {
      val fs   = fileSystem()
      fs.mkdirs(new Path(storePath))
      val out  = fs.create(tmpFile, /* overwrite */ true)
      val content = entries.map(toTsv).mkString("\n")
      out.write(content.getBytes("UTF-8"))
      out.close()
      // Atomic rename: replace registry file only after successful write.
      fs.delete(registryFile, false)
      fs.rename(tmpFile, registryFile)
    } catch {
      case NonFatal(e) =>
        logWarning(s"CTE-MV: failed to write persistent registry at $registryFile: $e")
    }
  }

  private def toTsv(e: CTEMVPersistentEntry): String =
    s"${e.fingerprint}\t${e.parquetPath}\t${e.globalViewName}\t${e.createdAt}"

  private def parseLine(line: String): Option[CTEMVPersistentEntry] = {
    val parts = line.split("\t", -1)
    if (parts.length != 4) None
    else
      try Some(CTEMVPersistentEntry(parts(0), parts(1), parts(2), parts(3).toLong))
      catch { case NonFatal(_) => None }
  }
}
