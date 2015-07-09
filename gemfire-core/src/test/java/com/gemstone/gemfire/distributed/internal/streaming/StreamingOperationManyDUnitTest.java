/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*=========================================================================
 * Copyright (c) 2007-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
//
//  StreamingOperationManyTest.java
//
package com.gemstone.gemfire.distributed.internal.streaming;

import java.util.*;
import dunit.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.distributed.internal.membership.*;
import com.gemstone.gemfire.internal.cache.Token;

public class StreamingOperationManyDUnitTest extends DistributedTestCase {

  public StreamingOperationManyDUnitTest(String name) {
    super(name);
  }

  public void testStreamingManyProvidersNoExceptions() throws Exception {
//    final String name = this.getUniqueName();

    // ask four other VMs to connect to the distributed system
    // this will be the data provider
    Host host = Host.getHost(0);
    for (int i = 0; i < 4; i++) {
      VM vm = host.getVM(i);
      vm.invoke(new SerializableRunnable("connect to system") {
        public void run() {
          assertTrue(getSystem() != null);
        }
      });
    }

    // get the other member id that connected
    // by getting the list of other member ids and
    Set setOfIds = getSystem().getDistributionManager().getOtherNormalDistributionManagerIds();
    assertEquals(4, setOfIds.size());
    TestStreamingOperationManyProviderNoExceptions streamOp = new TestStreamingOperationManyProviderNoExceptions(getSystem());
    streamOp.getDataFromAll(setOfIds);
    assertTrue(streamOp.dataValidated);
  }

  // about 100 chunks worth of integers?
  protected static final int NUM_INTEGERS = 32*1024 /* default socket buffer size*/ * 100 / 4;

  public static class TestStreamingOperationManyProviderNoExceptions extends StreamingOperation {
    volatile boolean dataValidated = false;
    ConcurrentMap senderMap = new ConcurrentHashMap();
    ConcurrentMap senderNumChunksMap = new ConcurrentHashMap();
    private int numChunks = -1; // made inst var to fix bug 37421

    public TestStreamingOperationManyProviderNoExceptions(InternalDistributedSystem sys) {
      super(sys);
    }

    protected DistributionMessage createRequestMessage(Set recipients, ReplyProcessor21 processor) {
      TestRequestStreamingMessageManyProviderNoExceptions msg = new TestRequestStreamingMessageManyProviderNoExceptions();
      msg.setRecipients(recipients);
      msg.processorId = processor==null? 0 : processor.getProcessorId();
      return msg;
    }

    protected synchronized boolean processData(List objects, InternalDistributedMember sender,
                                               int sequenceNum, boolean lastInSequence) {
      LogWriter logger = this.sys.getLogWriter();

      ConcurrentMap chunkMap = (ConcurrentMap)senderMap.get(sender);
      if (chunkMap == null) {
        chunkMap = new ConcurrentHashMap();
        ConcurrentMap chunkMap2 = (ConcurrentMap)this.senderMap.putIfAbsent(sender, chunkMap);
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
        prevValue = senderNumChunksMap.putIfAbsent(sender, new Integer(sequenceNum + 1)); // sequenceNum is 0-based
        if (prevValue != null) {
          logger.severe("prevValue != null");
        }
        // assert that we haven't gotten a true for lastInSequence yet
      }

//       logger.info("DEBUG processData: sender=" + sender
//                   + " objects.size=" + objects.size()
//                   + " seqNum=" + sequenceNum
//                   + " lastInSeq=" + lastInSequence
//                   + " chunkMap.size=" + chunkMap.size()
//                   + " numChunks=" + numChunks
//                   + " senderMap.size=" + senderMap.size());

      // are we completely done with all senders ?
      if (chunkMap.size() == numChunks  &&   // done with this sender
          senderMap.size() == 4) {           // we've heard from all 4 senders
//         logger.info("completely done (maybe)");
        boolean completelyDone = true;       // start with true assumption
        for (Iterator itr = senderMap.entrySet().iterator(); itr.hasNext(); ) {
          Map.Entry entry = (Map.Entry)itr.next();
          InternalDistributedMember senderV = (InternalDistributedMember)entry.getKey();
          ConcurrentMap chunkMapV = (ConcurrentMap)entry.getValue();
          Integer numChunksV = (Integer)senderNumChunksMap.get(senderV);
          if (chunkMapV == null) {
//             logger.info("Not completely done senderV=" + senderV
//                         + " chunkMapV==null");
            completelyDone = false;
            break;
          } else if (numChunksV == null) {
//             logger.info("Not completely done senderV=" + senderV
//                         + " numChunksV==null");
            completelyDone = false;
            break;
          } else if (chunkMapV.size() != numChunksV.intValue()) {
//             logger.info("Not completely done senderV=" + senderV
//                         + " chunkMapV.size=" + chunkMapV.size()
//                         + " numChunksV=" + numChunksV.intValue());
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
      LogWriter logger = this.sys.getLogWriter();
      for (Iterator senderItr = this.senderMap.entrySet().iterator(); senderItr.hasNext(); ) {
        Map.Entry entry = (Map.Entry)senderItr.next();
        ConcurrentMap chunkMap = (ConcurrentMap)entry.getValue();
        InternalDistributedMember sender = (InternalDistributedMember)entry.getKey();
        List[] arrayOfLists = new ArrayList[chunkMap.size()];
        List objList;
        int expectedInt = 0;

        // sort the input streams
        for (Iterator itr = chunkMap.entrySet().iterator(); itr.hasNext(); ) {
          Map.Entry entry2 = (Map.Entry)itr.next();
          int seqNum = ((Integer)entry2.getKey()).intValue();
          objList = (List)entry2.getValue();
          arrayOfLists[seqNum] = objList;
        }

        int count = 0;
        for (int i = 0; i < chunkMap.size(); i++) {
          Iterator itr = arrayOfLists[i].iterator();
          Integer nextInteger;
          while (itr.hasNext()) {
            nextInteger = (Integer)itr.next();
            if (nextInteger.intValue() != expectedInt) {
              logger.severe("nextInteger.intValue() != expectedInt");
              return;
            }
            expectedInt += 10; // the secret number is incremented by 10 each time
            count++;
          }
        }
        if (count != NUM_INTEGERS) {
          logger.severe("found " + count + " integers from " + sender + " , expected " + NUM_INTEGERS);
          return;
        }
        logger.info("Received " + count + " integers from " + sender + " in " + chunkMap.size() + " chunks");
      }
      dataValidated = true;
    }
  }

  public static final class TestRequestStreamingMessageManyProviderNoExceptions extends StreamingOperation.RequestStreamingMessage {
    private int nextInt = -10;
    private int count = 0;

    protected Object getNextReplyObject()
    throws ReplyException {
      if (++count > NUM_INTEGERS) {
        return Token.END_OF_STREAM;
      }
      nextInt += 10;
      return new Integer(nextInt);
    }
    public int getDSFID() {
      return NO_FIXED_ID;
    }
  }
}
