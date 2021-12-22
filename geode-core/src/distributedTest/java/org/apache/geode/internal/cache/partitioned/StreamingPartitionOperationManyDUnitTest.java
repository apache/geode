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
// StreamingPartitionOperationManyTest.java
//
package org.apache.geode.internal.cache.partitioned;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.RegionsTest;

@Category({RegionsTest.class})
public class StreamingPartitionOperationManyDUnitTest extends JUnit4CacheTestCase {

  /* SerializableRunnable object to create a PR */
  CacheSerializableRunnable createPrRegionWithDS_DACK =
      new CacheSerializableRunnable("createPrRegionWithDS") {

        @Override
        public void run2() throws CacheException {
          Cache cache = getCache();
          AttributesFactory attr = new AttributesFactory();
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          paf.setTotalNumBuckets(5);
          PartitionAttributes prAttr = paf.create();
          attr.setPartitionAttributes(prAttr);
          RegionAttributes regionAttribs = attr.create();
          cache.createRegion("PR1", regionAttribs);
        }
      };


  public StreamingPartitionOperationManyDUnitTest() {
    super();
  }

  @Test
  public void testStreamingManyProvidersNoExceptions() throws Exception {
    // final String name = this.getUniqueName();

    // ask four other VMs to connect to the distributed system
    // and create partitioned region; this will be the data provider
    Host host = Host.getHost(0);
    for (int i = 0; i < 4; i++) {
      VM vm = host.getVM(i);
      vm.invoke(new SerializableRunnable("connect to system") {
        @Override
        public void run() {
          assertTrue(getSystem() != null);
        }
      });
      vm.invoke(createPrRegionWithDS_DACK);
    }

    // also create the PR here so we can get the regionId
    createPrRegionWithDS_DACK.run2();

    int regionId = ((PartitionedRegion) getCache().getRegion("PR1")).getPRId();


    // get the other member id that connected
    // by getting the list of other member ids and
    Set setOfIds = getSystem().getDistributionManager().getOtherNormalDistributionManagerIds();
    assertEquals(4, setOfIds.size());
    TestStreamingPartitionOperationManyProviderNoExceptions streamOp =
        new TestStreamingPartitionOperationManyProviderNoExceptions(getSystem(), regionId);
    streamOp.getPartitionedDataFrom(setOfIds);
    assertTrue("data did not validate correctly: see log for severe message",
        streamOp.dataValidated);

  }


  // about 100 chunks worth of integers?
  protected static final int NUM_INTEGERS = 32 * 1024 /* default socket buffer size */ * 100 / 4;

  public static class TestStreamingPartitionOperationManyProviderNoExceptions
      extends StreamingPartitionOperation {
    volatile boolean dataValidated = false;
    ConcurrentMap senderMap = new ConcurrentHashMap();
    ConcurrentMap senderNumChunksMap = new ConcurrentHashMap();

    public TestStreamingPartitionOperationManyProviderNoExceptions(InternalDistributedSystem sys,
        int regionId) {
      super(sys, regionId);
    }

    @Override
    protected DistributionMessage createRequestMessage(Set recipients, ReplyProcessor21 processor) {
      TestStreamingPartitionMessageManyProviderNoExceptions msg =
          new TestStreamingPartitionMessageManyProviderNoExceptions(recipients, regionId,
              processor);
      return msg;
    }

    @Override
    protected synchronized boolean processData(List objects, InternalDistributedMember sender,
        int sequenceNum, boolean lastInSequence) {
      LogWriter logger = sys.getLogWriter();

      int numChunks = -1;

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
        prevValue = senderNumChunksMap.putIfAbsent(sender, new Integer(sequenceNum + 1)); // sequenceNum
                                                                                          // is
                                                                                          // 0-based
        // assert that we haven't gotten a true for lastInSequence yet
        if (prevValue != null) {
          logger.severe("prevValue != null");
        }
      }

      Integer numChunksI = (Integer) senderNumChunksMap.get(sender);
      if (numChunksI != null) {
        numChunks = numChunksI.intValue();
      }

      // are we completely done with all senders ?
      if (chunkMap.size() == numChunks && // done with this sender
          senderMap.size() == 4) { // we've heard from all 4 senders
        boolean completelyDone = true; // start with true assumption
        for (final Object o : senderMap.entrySet()) {
          Map.Entry entry = (Map.Entry) o;
          InternalDistributedMember senderV = (InternalDistributedMember) entry.getKey();
          ConcurrentMap chunkMapV = (ConcurrentMap) entry.getValue();
          Integer numChunksV = (Integer) senderNumChunksMap.get(senderV);
          if (chunkMapV == null || numChunksV == null
              || chunkMapV.size() != numChunksV.intValue()) {
            completelyDone = false;
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
      logger.info("Validating data...");
      try {
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
      } catch (Exception e) {
        logger.severe("Validation exception", e);
      }
      logger.info("Successful validation");
      dataValidated = true;
    }
  }

  public static class TestStreamingPartitionMessageManyProviderNoExceptions
      extends StreamingPartitionOperation.StreamingPartitionMessage {
    private int nextInt = -10;
    private int count = 0;

    public TestStreamingPartitionMessageManyProviderNoExceptions() {
      super();
    }

    public TestStreamingPartitionMessageManyProviderNoExceptions(Set recipients, int regionId,
        ReplyProcessor21 processor) {
      super(recipients, regionId, processor);
    }

    @Override
    protected Object getNextReplyObject(PartitionedRegion pr) throws ReplyException {
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
