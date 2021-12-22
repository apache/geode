/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

//
// StreamingOperationOneTest.java
//
package org.apache.geode.distributed.internal.streaming;

import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.junit.Test;

import org.apache.geode.LogWriter;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;


public class StreamingOperationOneDUnitTest extends JUnit4DistributedTestCase {

  @Test
  public void testStreamingOneProviderNoExceptions() throws Exception {
    // final String name = this.getUniqueName();

    // ask another VM to connect to the distributed system
    // this will be the data provider, and get their member id at the same time
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    class IDGetter implements Serializable {
      InternalDistributedMember getMemberId() {
        return getSystem().getDistributedMember();
      }
    }

    // get the other member id that connected
    InternalDistributedMember otherId =
        vm0.invoke(new IDGetter(), "getMemberId");
    Set setOfIds = Collections.singleton(otherId);

    TestStreamingOperationOneProviderNoExceptions streamOp =
        new TestStreamingOperationOneProviderNoExceptions(getSystem());
    streamOp.getDataFromAll(setOfIds);
    assertTrue(streamOp.dataValidated);
  }


  // about 100 chunks worth of integers?
  protected static final int NUM_INTEGERS = 32 * 1024 /* default socket buffer size */ * 100 / 4;

  public static class TestStreamingOperationOneProviderNoExceptions extends StreamingOperation {
    ConcurrentMap chunkMap = new ConcurrentHashMap();
    int numChunks = -1;
    volatile boolean dataValidated = false;

    public TestStreamingOperationOneProviderNoExceptions(InternalDistributedSystem sys) {
      super(sys);
    }

    @Override
    protected DistributionMessage createRequestMessage(Set recipients, ReplyProcessor21 processor) {
      TestRequestStreamingMessageOneProviderNoExceptions msg =
          new TestRequestStreamingMessageOneProviderNoExceptions();
      msg.processorId = processor == null ? 0 : processor.getProcessorId();
      msg.setRecipients(recipients);
      return msg;
    }

    @Override
    protected synchronized boolean processData(List objects, InternalDistributedMember sender,
        int sequenceNum, boolean lastInSequence) {
      LogWriter logger = sys.getLogWriter();

      // assert that we haven't gotten this sequence number yet
      Object prevValue = chunkMap.putIfAbsent(new Integer(sequenceNum), objects);
      if (prevValue != null) {
        logger.severe("prevValue != null");
      }

      if (lastInSequence) {
        // assert that we haven't gotten a true for lastInSequence yet
        if (numChunks != -1) {
          logger.severe("this.numChunks != -1");
        }
        numChunks = sequenceNum + 1; // sequenceNum is 0-based
      }

      if (chunkMap.size() == numChunks) {
        validateData();
      } else {

        // assert that we either don't know how many chunks we're going to get yet (-1)
        // or we haven't completed yet
        if (numChunks != -1 && chunkMap.size() >= numChunks) {
          logger.severe("this.numChunks != -1 && this.chunkMap.size() >= this.numChunks");
        }

        // assert that we aren't getting too many chunks
        if (chunkMap.size() >= 200) {
          logger.warning("this.chunkMap.size() >= 200");
        }
      }
      return true;
    }

    private void validateData() {
      List[] arrayOfLists = new ArrayList[numChunks];
      List objList;
      int expectedInt = 0;
      LogWriter logger = sys.getLogWriter();

      // sort the input streams
      for (final Object o : chunkMap.entrySet()) {
        Map.Entry entry = (Map.Entry) o;
        int seqNum = ((Integer) entry.getKey()).intValue();
        objList = (List) entry.getValue();
        arrayOfLists[seqNum] = objList;
      }

      int count = 0;
      for (int i = 0; i < numChunks; i++) {
        Iterator itr = arrayOfLists[i].iterator();
        Integer nextInteger;
        while (itr.hasNext()) {
          nextInteger = (Integer) itr.next();
          if (nextInteger.intValue() != expectedInt) {
            logger.severe("nextInteger.intValue() != expectedInt");
            return;
          }
          expectedInt += 10; // the secret number is incremented by 10 each time
          count++;
        }
      }
      if (count != NUM_INTEGERS) {
        logger.severe("found " + count + " integers, expected " + NUM_INTEGERS);
      } else {
        dataValidated = true;
        logger.info("Received " + count + " integers in " + numChunks + " chunks");
      }
    }
  }

  public static class TestRequestStreamingMessageOneProviderNoExceptions
      extends StreamingOperation.RequestStreamingMessage {
    private int nextInt = -10;
    private int count = 0;

    @Override
    protected Object getNextReplyObject() throws ReplyException {
      if (++count > NUM_INTEGERS) {
        return Token.END_OF_STREAM;
      }
      nextInt += 10;
      return new Integer(nextInt);
    }

    @Override
    public int getDSFID() {
      return NO_FIXED_ID;
    }
  }
}
