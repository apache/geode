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
package org.apache.geode.spark.connector.internal.geodefunctions

import java.util.concurrent.{TimeUnit, LinkedBlockingQueue, BlockingQueue}
import org.apache.geode.DataSerializer
import org.apache.geode.cache.execute.ResultCollector
import org.apache.geode.cache.query.internal.types.StructTypeImpl
import org.apache.geode.cache.query.types.StructType
import org.apache.geode.distributed.DistributedMember
import org.apache.geode.internal.{Version, ByteArrayDataInput}
import org.apache.geode.spark.connector.internal.geodefunctions.StructStreamingResultSender.
       {TYPE_CHUNK, DATA_CHUNK, ERROR_CHUNK, SER_DATA, UNSER_DATA, BYTEARR_DATA}

/**
 * StructStreamingResultCollector and StructStreamingResultSender are paired
 * to transfer result of list of `org.apache.geode.cache.query.Struct`
 * from Geode server to Spark Connector (the client of Geode server)
 * in streaming, i.e., while sender sending the result, the collector can
 * start processing the arrived result without waiting for full result to
 * become available.
 */
class StructStreamingResultCollector(desc: String) extends ResultCollector[Array[Byte], Iterator[Array[Object]]] {

  /** the constructor that provide default `desc` (description) */
  def this() = this("StructStreamingResultCollector")
  
  private val queue: BlockingQueue[Array[Byte]] = new LinkedBlockingQueue[Array[Byte]]()
  var structType: StructType = null

  /** ------------------------------------------ */
  /**  ResultCollector interface implementations */
  /** ------------------------------------------ */
  
  override def getResult: Iterator[Array[Object]] = resultIterator

  override def getResult(timeout: Long, unit: TimeUnit): Iterator[Array[Object]] = 
    throw new UnsupportedOperationException()

  /** addResult add non-empty byte array (chunk) to the queue */
  override def addResult(memberID: DistributedMember, chunk: Array[Byte]): Unit = 
    if (chunk != null && chunk.size > 1) {
      this.queue.add(chunk)
      // println(s"""$desc receive from $memberID: ${chunk.mkString(" ")}""")
    }

  /** endResults add special `Array.empty` to the queue as marker of end of data */
  override def endResults(): Unit = this.queue.add(Array.empty)
  
  override def clearResults(): Unit = this.queue.clear()

  /** ------------------------------------------ */
  /**             Internal methods               */
  /** ------------------------------------------ */

  def getResultType: StructType = {
    // trigger lazy resultIterator initialization if necessary
    if (structType == null)  resultIterator.hasNext
    structType        
  }

  /**
   * Note: The data is sent in chunks, and each chunk contains multiple 
   * records. So the result iterator is an iterator (I) of iterator (II),
   * i.e., go through each chunk (iterator (I)), and for each chunk, go 
   * through each record (iterator (II)). 
   */
  private lazy val resultIterator = new Iterator[Array[Object]] {

    private var currentIterator: Iterator[Array[Object]] = nextIterator()
    
    override def hasNext: Boolean = {
      if (!currentIterator.hasNext && currentIterator != Iterator.empty) currentIterator = nextIterator()
      currentIterator.hasNext
    }

    /** Note: make sure call `hasNext` first to adjust `currentIterator` */
    override def next(): Array[Object] = currentIterator.next()
  }
  
  /** get the iterator for the next chunk of data */
  private def nextIterator(): Iterator[Array[Object]] = {
    val chunk: Array[Byte] = queue.take
    if (chunk.isEmpty) {
      Iterator.empty
    } else {
      val input = new ByteArrayDataInput()
      input.initialize(chunk, Version.CURRENT)
      val chunkType = input.readByte()
      // println(s"chunk type $chunkType")
      chunkType match {
        case TYPE_CHUNK =>
          if (structType == null)
            structType = DataSerializer.readObject(input).asInstanceOf[StructTypeImpl]
          nextIterator()
        case DATA_CHUNK =>
          // require(structType != null && structType.getFieldNames.length > 0)
          if (structType == null) structType = StructStreamingResultSender.KeyValueType
          chunkToIterator(input, structType.getFieldNames.length)
        case ERROR_CHUNK => 
          val error = DataSerializer.readObject(input).asInstanceOf[Exception]
          errorPropagationIterator(error)
        case _ => throw new RuntimeException(s"unknown chunk type: $chunkType")
      }
    }
  }

  /** create a iterator that propagate sender's exception */
  private def errorPropagationIterator(ex: Exception) = new Iterator[Array[Object]] {
    val re = new RuntimeException(ex)
    override def hasNext: Boolean = throw re
    override def next(): Array[Object] = throw re
  }
  
  /** convert a chunk of data to an iterator */
  private def chunkToIterator(input: ByteArrayDataInput, rowSize: Int) = new Iterator[Array[Object]] {
    override def hasNext: Boolean = input.available() > 0
    val tmpInput = new ByteArrayDataInput()
    override def next(): Array[Object] = 
      (0 until rowSize).map { ignore =>
        val b = input.readByte()
        b match {
          case SER_DATA => 
            val arr: Array[Byte] = DataSerializer.readByteArray(input)
            tmpInput.initialize(arr, Version.CURRENT)
            DataSerializer.readObject(tmpInput).asInstanceOf[Object]
          case UNSER_DATA =>
            DataSerializer.readObject(input).asInstanceOf[Object]
          case BYTEARR_DATA =>
            DataSerializer.readByteArray(input).asInstanceOf[Object]
          case _ => 
            throw new RuntimeException(s"unknown data type $b")
        }
      }.toArray
  }
  
}

