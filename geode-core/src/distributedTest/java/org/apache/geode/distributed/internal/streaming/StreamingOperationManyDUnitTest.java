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
// StreamingOperationManyTest.java
//
package org.apache.geode.distributed.internal.streaming;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
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


public class StreamingOperationManyDUnitTest extends JUnit4DistributedTestCase {

  @Test
  public void testStreamingManyProvidersNoExceptions() throws Exception {
    // final String name = this.getUniqueName();

    // ask four other VMs to connect to the distributed system
    // this will be the data provider
    Host host = Host.getHost(0);
    HashSet<InternalDistributedMember> otherMemberIds = new HashSet<>();
    for (int i = 0; i < 4; i++) {
      VM vm = host.getVM(i);
      otherMemberIds.add(vm.invoke(() -> getSystem().getDistributedMember()));
    }

    TestStreamingOperationManyProviderNoExceptions streamOp =
        new TestStreamingOperationManyProviderNoExceptions(getSystem());
    streamOp.getDataFromAll(otherMemberIds);
    assertTrue(streamOp.dataValidated);
  }

  // about 100 chunks worth of integers?
  protected static final int NUM_INTEGERS = 32 * 1024 /* default socket buffer size */ * 100 / 4;

  public static class TestStreamingOperationManyProviderNoExceptions extends StreamingOperation {
    volatile boolean dataValidated = false;
    ConcurrentMap senderMap = new ConcurrentHashMap();
    ConcurrentMap senderNumChunksMap = new ConcurrentHashMap();
    private int numChunks = -1; // made inst var to fix bug 37421

    public TestStreamingOperationManyProviderNoExceptions(InternalDistributedSystem sys) {
      super(sys);
    }

    @Override
    protected DistributionMessage createRequestMessage(Set recipients, ReplyProcessor21 processor) {
      TestRequestStreamingMessageManyProviderNoExceptions msg =
          new TestRequestStreamingMessageManyProviderNoExceptions();
      msg.setRecipients(recipients);
      msg.processorId = processor == null ? 0 : processor.getProcessorId();
      return msg;
    }

    @Override
    protected synchronized boolean processData(List objects, InternalDistributedMember sender,
        int sequenceNum, boolean lastInSequence) {
      LogWriter logger = sys.getLogWriter();

      ConcurrentMap chunkMap = (ConcurrentMap) senderMap.get(sender);
      if (chunkMap == null) {
        chunkMap = new ConcurrentHashMap();
        ConcurrentMap chunkMap2 = (ConcurrentMap) senderMap.putIfAbsent(sender, chunkMap);
        if (chunkMap2 != null) {
          chunkMap = chunkMap2;
        }
      }

      // assert that we haven't gotten this sequence number yet
      Object prevValue = chunkMap.putIfAbsent(new Integer(sequenceNum), objects);
      if (prevValue != null) {
        logger.severe("prevValue != null");
      }

      if (lastInSequence) {
        numChunks = sequenceNum + 1;
        prevValue = senderNumChunksMap.putIfAbsent(sender, new Integer(sequenceNum + 1)); // sequenceNum
                                                                                          // is
                                                                                          // 0-based
        if (prevValue != null) {
          logger.severe("prevValue != null");
        }
        // assert that we haven't gotten a true for lastInSequence yet
      }

      // logger.info("DEBUG processData: sender=" + sender
      // + " objects.size=" + objects.size()
      // + " seqNum=" + sequenceNum
      // + " lastInSeq=" + lastInSequence
      // + " chunkMap.size=" + chunkMap.size()
      // + " numChunks=" + numChunks
      // + " senderMap.size=" + senderMap.size());

      // are we completely done with all senders ?
      if (chunkMap.size() == numChunks && // done with this sender
          senderMap.size() == 4) { // we've heard from all 4 senders
        // logger.info("completely done (maybe)");
        boolean completelyDone = true; // start with true assumption
        for (final Object o : senderMap.entrySet()) {
          Map.Entry entry = (Map.Entry) o;
          InternalDistributedMember senderV = (InternalDistributedMember) entry.getKey();
          ConcurrentMap chunkMapV = (ConcurrentMap) entry.getValue();
          Integer numChunksV = (Integer) senderNumChunksMap.get(senderV);
          if (chunkMapV == null) {
            // logger.info("Not completely done senderV=" + senderV
            // + " chunkMapV==null");
            completelyDone = false;
            break;
          } else if (numChunksV == null) {
            // logger.info("Not completely done senderV=" + senderV
            // + " numChunksV==null");
            completelyDone = false;
            break;
          } else if (chunkMapV.size() != numChunksV.intValue()) {
            // logger.info("Not completely done senderV=" + senderV
            // + " chunkMapV.size=" + chunkMapV.size()
            // + " numChunksV=" + numChunksV.intValue());
            completelyDone = false;
            break;
          }
        }
        if (completelyDone) {
          validateData();
        }
      }

      return true;
    }

    private void validateData() {
      LogWriter logger = sys.getLogWriter();
      for (final Object value : senderMap.entrySet()) {
        Map.Entry entry = (Map.Entry) value;
        ConcurrentMap chunkMap = (ConcurrentMap) entry.getValue();
        InternalDistributedMember sender = (InternalDistributedMember) entry.getKey();
        List[] arrayOfLists = new ArrayList[chunkMap.size()];
        List objList;
        int expectedInt = 0;

        // sort the input streams
        for (final Object o : chunkMap.entrySet()) {
          Map.Entry entry2 = (Map.Entry) o;
          int seqNum = ((Integer) entry2.getKey()).intValue();
          objList = (List) entry2.getValue();
          arrayOfLists[seqNum] = objList;
        }

        int count = 0;
        for (int i = 0; i < chunkMap.size(); i++) {
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
          logger.severe(
              "found " + count + " integers from " + sender + " , expected " + NUM_INTEGERS);
          return;
        }
        logger.info("Received " + count + " integers from " + sender + " in " + chunkMap.size()
            + " chunks");
      }
      dataValidated = true;
    }
  }

  public static class TestRequestStreamingMessageManyProviderNoExceptions
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
