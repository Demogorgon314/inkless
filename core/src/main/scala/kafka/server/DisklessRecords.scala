/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import org.apache.kafka.common.record.internal.{FileRecords, MemoryRecords, MutableRecordBatch, Records}
import org.apache.kafka.common.utils.ByteBufferOutputStream

/**
 * Converts the records that a diskless engine returns. Engines may return any in-memory `Records`
 * implementation, so Kafka copies batches instead of depending on the engine's type.
 */
object DisklessRecords {
  /** Returns one buffer holding the prefix followed by the batches of the tail. */
  def concat(prefix: MemoryRecords, tail: Records): MemoryRecords = {
    val out = new ByteBufferOutputStream(prefix.sizeInBytes + tail.sizeInBytes)
    out.write(prefix.buffer().duplicate())
    writeBatches(out, tail)
    readable(out)
  }

  /** Returns the records as `MemoryRecords`, copying only when the engine used another type. */
  def toMemoryRecords(records: Records): MemoryRecords = records match {
    case memory: MemoryRecords => memory
    case other =>
      val out = new ByteBufferOutputStream(other.sizeInBytes)
      writeBatches(out, other)
      readable(out)
  }

  /** Sets the leader epoch of every in-memory batch; file-backed records stay untouched. */
  def setPartitionLeaderEpoch(records: Records, epoch: Int): Unit = records match {
    case _: FileRecords =>
    case other => other.batches().forEach {
      case batch: MutableRecordBatch => batch.setPartitionLeaderEpoch(epoch)
      case _ =>
    }
  }

  private def writeBatches(out: ByteBufferOutputStream, records: Records): Unit =
    records.batches().forEach {
      case batch: MutableRecordBatch => batch.writeTo(out)
      case batch => throw new IllegalArgumentException(s"Unsupported record batch type ${batch.getClass.getName}")
    }

  private def readable(out: ByteBufferOutputStream): MemoryRecords = {
    val buffer = out.buffer()
    buffer.flip()
    MemoryRecords.readableRecords(buffer)
  }
}
