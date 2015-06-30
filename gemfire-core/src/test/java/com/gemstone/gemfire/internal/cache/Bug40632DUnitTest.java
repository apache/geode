/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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

import dunit.Host;
import dunit.SerializableCallable;
import dunit.SerializableRunnable;
import dunit.VM;

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
