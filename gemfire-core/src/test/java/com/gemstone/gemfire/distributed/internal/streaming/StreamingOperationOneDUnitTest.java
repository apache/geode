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
//  StreamingOperationOneTest.java
//
package com.gemstone.gemfire.distributed.internal.streaming;

import java.util.*;
import java.io.*;
import dunit.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.distributed.internal.membership.*;
import com.gemstone.gemfire.internal.cache.Token;


public class StreamingOperationOneDUnitTest extends DistributedTestCase {

  public StreamingOperationOneDUnitTest(String name) {
    super(name);
  }

  public void testStreamingOneProviderNoExceptions() throws Exception {
//    final String name = this.getUniqueName();

    // ask another VM to connect to the distributed system
    // this will be the data provider, and get their member id at the same time
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    class IDGetter implements Serializable {
      InternalDistributedMember getMemberId() {
        return (InternalDistributedMember)getSystem().getDistributedMember();
      }
    }

    // get the other member id that connected
    InternalDistributedMember otherId = (InternalDistributedMember)vm0.invoke(new IDGetter(), "getMemberId");
    Set setOfIds = Collections.singleton(otherId);

    TestStreamingOperationOneProviderNoExceptions streamOp = new TestStreamingOperationOneProviderNoExceptions(getSystem());
    streamOp.getDataFromAll(setOfIds);
    assertTrue(streamOp.dataValidated);
  }


  // about 100 chunks worth of integers?
  protected static final int NUM_INTEGERS = 32*1024 /* default socket buffer size*/ * 100 / 4;

  public static class TestStreamingOperationOneProviderNoExceptions extends StreamingOperation {
    ConcurrentMap chunkMap = new ConcurrentHashMap();
    int numChunks = -1;
    volatile boolean dataValidated = false;

    public TestStreamingOperationOneProviderNoExceptions(InternalDistributedSystem sys) {
      super(sys);
    }

    protected DistributionMessage createRequestMessage(Set recipients, ReplyProcessor21 processor) {
      TestRequestStreamingMessageOneProviderNoExceptions msg = new TestRequestStreamingMessageOneProviderNoExceptions();
      msg.processorId = processor==null? 0 : processor.getProcessorId();
      msg.setRecipients(recipients);
      return msg;
    }

    protected synchronized boolean processData(List objects, InternalDistributedMember sender, int sequenceNum, boolean lastInSequence) {
      LogWriter logger = this.sys.getLogWriter();

      // assert that we haven't gotten this sequence number yet
      Object prevValue = this.chunkMap.putIfAbsent(new Integer(sequenceNum), objects);
      if (prevValue != null) {
        logger.severe("prevValue != null");
      }

      if (lastInSequence) {
        // assert that we haven't gotten a true for lastInSequence yet
        if (this.numChunks != -1) {
          logger.severe("this.numChunks != -1");
        }
        this.numChunks = sequenceNum + 1; // sequenceNum is 0-based
      }

      if (chunkMap.size() == this.numChunks) {
        validateData();
      } else {

        // assert that we either don't know how many chunks we're going to get yet (-1)
        // or we haven't completed yet
        if (this.numChunks != -1 && this.chunkMap.size() >= this.numChunks) {
          logger.severe("this.numChunks != -1 && this.chunkMap.size() >= this.numChunks");
        }

        // assert that we aren't getting too many chunks
        if (this.chunkMap.size() >= 200) {
          logger.warning("this.chunkMap.size() >= 200");
        }
      }
      return true;
    }

    private void validateData() {
      List[] arrayOfLists = new ArrayList[this.numChunks];
      List objList;
      int expectedInt = 0;
      LogWriter logger = this.sys.getLogWriter();

      // sort the input streams
      for (Iterator itr = this.chunkMap.entrySet().iterator(); itr.hasNext(); ) {
        Map.Entry entry = (Map.Entry)itr.next();
        int seqNum = ((Integer)entry.getKey()).intValue();
        objList = (List)entry.getValue();
        arrayOfLists[seqNum] = objList;
      }

      int count = 0;
      for (int i = 0; i < this.numChunks; i++) {
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
        logger.severe("found " + count + " integers, expected " + NUM_INTEGERS);
      }
      else {
        this.dataValidated = true;
        logger.info("Received " + count + " integers in " + this.numChunks + " chunks");
      }
    }
  }

  public static final class TestRequestStreamingMessageOneProviderNoExceptions extends StreamingOperation.RequestStreamingMessage {
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
