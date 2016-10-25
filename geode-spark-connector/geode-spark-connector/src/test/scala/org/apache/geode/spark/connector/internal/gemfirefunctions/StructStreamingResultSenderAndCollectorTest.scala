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

import org.apache.geode.DataSerializer
import org.apache.geode.cache.execute.{ResultCollector, ResultSender}
import org.apache.geode.cache.query.internal.types.{ObjectTypeImpl, StructTypeImpl}
import org.apache.geode.cache.query.types.ObjectType
import org.apache.geode.internal.{Version, ByteArrayDataInput, HeapDataOutputStream}
import org.apache.geode.internal.cache.{CachedDeserializable, CachedDeserializableFactory}
import org.scalatest.{BeforeAndAfter, FunSuite}
import scala.collection.JavaConversions._
import scala.concurrent.{Await, ExecutionContext, Future}
import ExecutionContext.Implicits.global
import scala.concurrent.duration._

class StructStreamingResultSenderAndCollectorTest extends FunSuite with BeforeAndAfter  {

  /** 
    * A test ResultSender that connects struct ResultSender and ResultCollector 
    * Note: this test ResultSender has to copy the data (byte array) since the
    *       StructStreamingResultSender will reuse the byte array.
    */
  class LocalResultSender(collector: ResultCollector[Array[Byte], _], num: Int = 1) extends ResultSender[Object] {

    var finishedNum = 0
    
    override def sendResult(result: Object): Unit =
      collector.addResult(null, result.asInstanceOf[Array[Byte]].clone())

    /** exception should be sent via lastResult() */
    override def sendException(throwable: Throwable): Unit = 
      throw new UnsupportedOperationException("sendException is not supported.")

    override def lastResult(result: Object): Unit = {
      collector.addResult(null, result.asInstanceOf[Array[Byte]].clone())
      this.synchronized {
        finishedNum += 1
        if (finishedNum == num)
          collector.endResults()
      }
    }
  }

  /** common variables */
  var collector: StructStreamingResultCollector = _
  var baseSender: LocalResultSender = _
  /** common types */
  val objType = new ObjectTypeImpl("java.lang.Object").asInstanceOf[ObjectType]
  val TwoColType = new StructTypeImpl(Array("key", "value"), Array(objType, objType))
  val OneColType = new StructTypeImpl(Array("value"), Array(objType))

  before {
    collector = new StructStreamingResultCollector
    baseSender = new LocalResultSender(collector, 1)
  }
  
  test("transfer simple data") {
    verifySimpleTransfer(sendDataType = true)
  }

  test("transfer simple data with no type info") {
    verifySimpleTransfer(sendDataType = false)
  }

  def verifySimpleTransfer(sendDataType: Boolean): Unit = {
    val iter = (0 to 9).map(i => Array(i.asInstanceOf[Object], (i.toString * 5).asInstanceOf[Object])).toIterator
    val dataType = if (sendDataType) TwoColType else null
    new StructStreamingResultSender(baseSender, dataType , iter).send()
    // println("type: " + collector.getResultType.toString)
    assert(TwoColType.equals(collector.getResultType))
    val iter2 = collector.getResult
    (0 to 9).foreach { i =>
      assert(iter2.hasNext)
      val o = iter2.next()
      assert(o.size == 2)
      assert(o(0).asInstanceOf[Int] == i)
      assert(o(1).asInstanceOf[String] == i.toString * 5)
    }
    assert(! iter2.hasNext)
  }

  
  /**
   * A test iterator that generate integer data
   * @param start the 1st value
   * @param n number of integers generated
   * @param genExcp generate Exception if true. This is used to test exception handling.
   */
  def intIterator(start: Int, n: Int, genExcp: Boolean): Iterator[Array[Object]] = {
    new Iterator[Array[Object]] {
      val max = if (genExcp) start + n else start + n - 1
      var index: Int = start - 1

      override def hasNext: Boolean = if (index < max) true else false

      override def next(): Array[Object] =
        if (index < (start + n - 1)) {
          index += 1
          Array(index.asInstanceOf[Object])
        } else throw new RuntimeException("simulated error")
    }
  }

  test("transfer data with 0 row") {
    new StructStreamingResultSender(baseSender, OneColType, intIterator(1, 0, genExcp = false)).send()
    // println("type: " + collector.getResultType.toString)
    assert(collector.getResultType == null)
    val iter = collector.getResult
    assert(! iter.hasNext)
  }

  test("transfer data with 10K rows") {
    new StructStreamingResultSender(baseSender, OneColType, intIterator(1, 10000, genExcp = false)).send()
    // println("type: " + collector.getResultType.toString)
    assert(OneColType.equals(collector.getResultType))
    val iter = collector.getResult
    // println(iter.toList.map(list => list.mkString(",")).mkString("; "))
    (1 to 10000).foreach { i =>
      assert(iter.hasNext)
      val o = iter.next()
      assert(o.size == 1)
      assert(o(0).asInstanceOf[Int] == i)
    }
    assert(! iter.hasNext)
  }

  test("transfer data with 10K rows with 2 sender") {
    baseSender = new LocalResultSender(collector, 2)
    val total = 300
    val sender1 = Future { new StructStreamingResultSender(baseSender, OneColType, intIterator(1, total/2, genExcp = false), "sender1").send()}
    val sender2 = Future { new StructStreamingResultSender(baseSender, OneColType, intIterator(total/2+1, total/2, genExcp = false), "sender2").send()}
    Await.result(sender1, 1.seconds)
    Await.result(sender2, 1.seconds)

    // println("type: " + collector.getResultType.toString)
    assert(OneColType.equals(collector.getResultType))
    val iter = collector.getResult
    // println(iter.toList.map(list => list.mkString(",")).mkString("; "))
    val set = scala.collection.mutable.Set[Int]()
    (1 to total).foreach { i =>
      assert(iter.hasNext)
      val o = iter.next()
      assert(o.size == 1)
      assert(! set.contains(o(0).asInstanceOf[Int]))
      set.add(o(0).asInstanceOf[Int])
    }
    assert(! iter.hasNext)
  }

  test("transfer data with 10K rows with 2 sender with error") {
    baseSender = new LocalResultSender(collector, 2)
    val total = 1000
    val sender1 = Future { new StructStreamingResultSender(baseSender, OneColType, intIterator(1, total/2, genExcp = false), "sender1").send()}
    val sender2 = Future { new StructStreamingResultSender(baseSender, OneColType, intIterator(total/2+1, total/2, genExcp = true), "sender2").send()}
    Await.result(sender1, 1 seconds)
    Await.result(sender2, 1 seconds)

    // println("type: " + collector.getResultType.toString)
    assert(OneColType.equals(collector.getResultType))
    val iter = collector.getResult
    // println(iter.toList.map(list => list.mkString(",")).mkString("; "))
    val set = scala.collection.mutable.Set[Int]()
    intercept[RuntimeException] {
      (1 to total).foreach { i =>
        assert(iter.hasNext)
        val o = iter.next()
        assert(o.size == 1)
        assert(! set.contains(o(0).asInstanceOf[Int]))
        set.add(o(0).asInstanceOf[Int])
      }
    }
    // println(s"rows received: ${set.size}")
  }

  test("transfer data with Exception") {
    new StructStreamingResultSender(baseSender, OneColType, intIterator(1, 200, genExcp = true)).send()
    // println("type: " + collector.getResultType.toString)
    val iter = collector.getResult
    intercept[RuntimeException] ( iter.foreach(_.mkString(",")) )
  }

  def stringPairIterator(n: Int, genExcp: Boolean): Iterator[Array[Object]] =
    intIterator(1, n, genExcp).map(x => Array(s"key-${x(0)}", s"value-${x(0)}"))

  test("transfer string pair data with 200 rows") {
    new StructStreamingResultSender(baseSender, TwoColType, stringPairIterator(1000, genExcp = false)).send()
    // println("type: " + collector.getResultType.toString)
    assert(TwoColType.equals(collector.getResultType))
    val iter = collector.getResult
    // println(iter.toList.map(list => list.mkString(",")).mkString("; "))
    (1 to 1000).foreach { i =>
      assert(iter.hasNext)
      val o = iter.next()
      assert(o.size == 2)
      assert(o(0) == s"key-$i")
      assert(o(1) == s"value-$i")
    }
    assert(! iter.hasNext)
  }

  /**
   * Usage notes: There are 3 kinds of data to transfer:
   * (1) object, (2) byte array of serialized object, and (3) byte array
   * this test shows how to handle all of them.
   */
  test("DataSerializer usage") {
    val outBuf = new HeapDataOutputStream(1024, null)
    val inBuf = new ByteArrayDataInput()

    // 1. a regular object
    val hello = "Hello World!" * 30
    // serialize the data
    DataSerializer.writeObject(hello, outBuf)
    val bytesHello = outBuf.toByteArray.clone()
    // de-serialize the data
    inBuf.initialize(bytesHello, Version.CURRENT)
    val hello2 = DataSerializer.readObject(inBuf).asInstanceOf[Object]
    assert(hello == hello2)
    
    // 2. byte array of serialized object
    // serialize: byte array from `CachedDeserializable`
    val cd: CachedDeserializable = CachedDeserializableFactory.create(bytesHello)
    outBuf.reset()
    DataSerializer.writeByteArray(cd.getSerializedValue, outBuf)
    // de-serialize the data in 2 steps
    inBuf.initialize(outBuf.toByteArray.clone(), Version.CURRENT)
    val bytesHello2: Array[Byte] = DataSerializer.readByteArray(inBuf)
    inBuf.initialize(bytesHello2, Version.CURRENT)
    val hello3 = DataSerializer.readObject(inBuf).asInstanceOf[Object]
    assert(hello == hello3)

    // 3. byte array
    outBuf.reset()
    DataSerializer.writeByteArray(bytesHello, outBuf)
    inBuf.initialize(outBuf.toByteArray.clone(), Version.CURRENT)
    val bytesHello3: Array[Byte] = DataSerializer.readByteArray(inBuf)
    assert(bytesHello sameElements bytesHello3)
  }
}
