/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.spark.connector.internal.oql

import java.util.concurrent.{TimeUnit, LinkedBlockingDeque}

import org.apache.geode.DataSerializer
import org.apache.geode.cache.execute.ResultCollector
import org.apache.geode.distributed.DistributedMember
import org.apache.geode.internal.{Version, ByteArrayDataInput}

class QueryResultCollector extends ResultCollector[Array[Byte], Iterator[Object]]{

  private val queue = new LinkedBlockingDeque[Array[Byte]]()

  override def getResult = resultIterator

  override def getResult(timeout: Long, unit: TimeUnit) = throw new UnsupportedOperationException

  override def addResult(memberID: DistributedMember , chunk: Array[Byte]) =
    if (chunk != null && chunk.size > 0) {
      queue.add(chunk)
    }

  override def endResults = queue.add(Array.empty)


  override def clearResults = queue.clear

  private lazy val resultIterator = new Iterator[Object] {
    private var currentIterator = nextIterator
    def hasNext = {
      if (!currentIterator.hasNext && currentIterator != Iterator.empty)
        currentIterator = nextIterator
      currentIterator.hasNext
    }
    def next = currentIterator.next
  }

  private def nextIterator: Iterator[Object] = {
    val chunk = queue.take
    if (chunk.isEmpty) {
      Iterator.empty
    }
    else {
      val input = new ByteArrayDataInput
      input.initialize(chunk, Version.CURRENT)
      new Iterator[Object] {
        override def hasNext: Boolean = input.available() > 0
        override def next: Object = DataSerializer.readObject(input).asInstanceOf[Object]
      }
    }
  }

}
