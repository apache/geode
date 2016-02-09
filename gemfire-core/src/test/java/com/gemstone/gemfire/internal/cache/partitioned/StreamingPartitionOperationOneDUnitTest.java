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

//
//  StreamingPartitionOperationOneTest.java
//
package com.gemstone.gemfire.internal.cache.partitioned;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.Token;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

public class StreamingPartitionOperationOneDUnitTest extends CacheTestCase {

	/* SerializableRunnable object to create PR */
	CacheSerializableRunnable createPrRegionWithDS_DACK = new CacheSerializableRunnable("createPrRegionWithDS") {

		public void run2() throws CacheException {
			Cache cache = getCache();
			AttributesFactory attr = new AttributesFactory();
			PartitionAttributesFactory paf = new PartitionAttributesFactory();
			paf.setTotalNumBuckets(5);
			PartitionAttributes prAttr = paf.create();
			attr.setPartitionAttributes(prAttr);
			RegionAttributes regionAttribs = attr.create();
			cache.createRegion("PR1",
                                                    regionAttribs);
		}
	};

  class IDGetter implements Serializable {
    InternalDistributedMember getMemberId() {
      return (InternalDistributedMember)getSystem().getDistributedMember();
    }
  }


  public StreamingPartitionOperationOneDUnitTest(String name) {
    super(name);
  }

  public void testStreamingPartitionOneProviderNoExceptions() throws Exception {
//    final String name = this.getUniqueName();

    // ask another VM to connect to the distributed system
    // this will be the data provider, and get their member id at the same time
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    // get the other member id that connected
    InternalDistributedMember otherId = (InternalDistributedMember)vm0.invoke(new IDGetter(), "getMemberId");

    vm0.invoke(createPrRegionWithDS_DACK);

    // also create the PR here so we can get the regionId
    createPrRegionWithDS_DACK.run2();

    int regionId = ((PartitionedRegion)getCache().getRegion("PR1")).getPRId();

    Set setOfIds = Collections.singleton(otherId);

    TestStreamingPartitionOperationOneProviderNoExceptions streamOp = new TestStreamingPartitionOperationOneProviderNoExceptions(getSystem(), regionId);
    try {
      streamOp.getPartitionedDataFrom(setOfIds);
    }
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch (Throwable t) {
      Assert.fail("getPartitionedDataFrom failed", t);
    }
    assertTrue(streamOp.dataValidated);
  }


  // about 100 chunks worth of integers?
  protected static final int NUM_INTEGERS = 32*1024 /* default socket buffer size*/ * 100 / 4;

  public static class TestStreamingPartitionOperationOneProviderNoExceptions extends StreamingPartitionOperation {
    ConcurrentMap chunkMap = new ConcurrentHashMap();
    int numChunks = -1;
    volatile boolean dataValidated = false;

    public TestStreamingPartitionOperationOneProviderNoExceptions(InternalDistributedSystem sys, int regionId) {
      super(sys, regionId);
    }

    protected DistributionMessage createRequestMessage(Set recipients, ReplyProcessor21 processor) {
      TestStreamingPartitionMessageOneProviderNoExceptions msg = new TestStreamingPartitionMessageOneProviderNoExceptions(recipients, this.regionId, processor);
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

  public static final class TestStreamingPartitionMessageOneProviderNoExceptions extends StreamingPartitionOperation.StreamingPartitionMessage {
    private int nextInt = -10;
    private int count = 0;

    public TestStreamingPartitionMessageOneProviderNoExceptions() {
      super();
    }

    public TestStreamingPartitionMessageOneProviderNoExceptions(Set recipients, int regionId, ReplyProcessor21 processor) {
      super(recipients, regionId, processor);
    }


    protected Object getNextReplyObject(PartitionedRegion pr)
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
