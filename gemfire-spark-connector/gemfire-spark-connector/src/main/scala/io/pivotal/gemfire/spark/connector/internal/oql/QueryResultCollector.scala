package io.pivotal.gemfire.spark.connector.internal.oql

import java.util.concurrent.{TimeUnit, LinkedBlockingDeque}

import com.gemstone.gemfire.DataSerializer
import com.gemstone.gemfire.cache.execute.ResultCollector
import com.gemstone.gemfire.distributed.DistributedMember
import com.gemstone.gemfire.internal.{Version, ByteArrayDataInput}

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