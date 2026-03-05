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

import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.sql.catalyst.plans.NormalizePlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Computes a stable fingerprint for a logical plan by normalizing ExprIds, CTE IDs,
 * join conditions and filter predicates before hashing.
 */
object CTEFingerprint {
  def compute(plan: LogicalPlan): String = {
    val normalized = NormalizePlan(plan)
    val bytes = MessageDigest.getInstance("SHA-256")
      .digest(normalized.treeString.getBytes("UTF-8"))
    bytes.map("%02x".format(_)).mkString.take(32)
  }
}

/**
 * Metadata for a materialized CTE view entry.
 *
 * @param fingerprint The stable plan fingerprint.
 * @param viewName    The temp view name under which the materialized data is registered.
 * @param createdAt   Epoch millis when the entry was created.
 */
case class CTEMVEntry(fingerprint: String, viewName: String, createdAt: Long)

/**
 * Per-session registry that tracks which CTE bodies have been materialized.
 * Thread-safe via ConcurrentHashMap.
 */
class CTEMVRegistry {
  private val map = new ConcurrentHashMap[String, CTEMVEntry]()

  def lookup(fp: String): Option[CTEMVEntry] = Option(map.get(fp))

  def register(fp: String, entry: CTEMVEntry): Unit = map.put(fp, entry)

  def contains(fp: String): Boolean = map.containsKey(fp)

  def entries(): Seq[CTEMVEntry] = {
    import scala.jdk.CollectionConverters._
    map.values().asScala.toSeq
  }
}
