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
package com.gemstone.gemfire.internal.cache;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.TestCase;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.ExpirationAction;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.cache.control.ResourceManager;
import com.gemstone.gemfire.cache.partition.PartitionMemberInfo;
import com.gemstone.gemfire.cache.partition.PartitionRebalanceInfo;
import com.gemstone.gemfire.cache.partition.PartitionRegionInfo;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PRHARedundancyProvider;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.cache.control.ResourceManagerStats;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.ResourceObserverAdapter;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * @author dsmith
 *
 */
public class Bug40632DUnitTest extends CacheTestCase {

  /**
   * @param name
   */
  public Bug40632DUnitTest(String name) {
    super(name);
  }
  
  public void testLocalDestroyIdleTimeout() {
    Cache cache = getCache();
    AttributesFactory attr = new AttributesFactory();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1);
    paf.setRecoveryDelay(-1);
    paf.setStartupRecoveryDelay(-1);
    PartitionAttributes prAttr = paf.create();
    attr.setStatisticsEnabled(true);
    attr.setEntryIdleTimeout(new ExpirationAttributes(1000, ExpirationAction.LOCAL_DESTROY));
    attr.setPartitionAttributes(prAttr);
    try {
      cache.createRegion("region1", attr.create());
      fail("We should not have been able to create the region");
    } catch(IllegalStateException expected) {
      
    }
  }
  
  public void testLocalDestroyTimeToLive() {
    Cache cache = getCache();
    AttributesFactory attr = new AttributesFactory();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1);
    paf.setRecoveryDelay(-1);
    paf.setStartupRecoveryDelay(-1);
    PartitionAttributes prAttr = paf.create();
    attr.setStatisticsEnabled(true);
    attr.setEntryTimeToLive(new ExpirationAttributes(1000, ExpirationAction.LOCAL_DESTROY));
    attr.setPartitionAttributes(prAttr);
    try {
    cache.createRegion("region1", attr.create());
    fail("We should not have been able to create the region");
    } catch(IllegalStateException expected) {
      
    }
  }
  
  public void testLocalInvalidateIdleTimeout() {
    Cache cache = getCache();
    AttributesFactory attr = new AttributesFactory();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1);
    paf.setRecoveryDelay(-1);
    paf.setStartupRecoveryDelay(-1);
    PartitionAttributes prAttr = paf.create();
    attr.setStatisticsEnabled(true);
    attr.setEntryIdleTimeout(new ExpirationAttributes(1000, ExpirationAction.LOCAL_INVALIDATE));
    attr.setPartitionAttributes(prAttr);
    try {
    cache.createRegion("region1", attr.create());
    fail("We should not have been able to create the region");
    } catch(IllegalStateException expected) {
      
    }
  }
  
  public void testLocalInvalidateTimeToLive() {
    Cache cache = getCache();
    AttributesFactory attr = new AttributesFactory();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1);
    paf.setRecoveryDelay(-1);
    paf.setStartupRecoveryDelay(-1);
    PartitionAttributes prAttr = paf.create();
    attr.setStatisticsEnabled(true);
    attr.setEntryTimeToLive(new ExpirationAttributes(1000, ExpirationAction.LOCAL_INVALIDATE));
    attr.setPartitionAttributes(prAttr);
    try {
    cache.createRegion("region1", attr.create());
    fail("We should not have been able to create the region");
    } catch(IllegalStateException expected) {
      
    }
  }
}
