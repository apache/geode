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

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.ExpirationAction;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.test.dunit.cache.internal.JUnit4CacheTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class Bug40632DUnitTest extends JUnit4CacheTestCase {

  @Test
  public void testLocalDestroyIdleTimeout() throws Exception {
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
  
  @Test
  public void testLocalDestroyTimeToLive() throws Exception {
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
  
  @Test
  public void testLocalInvalidateIdleTimeout() throws Exception {
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
  
  @Test
  public void testLocalInvalidateTimeToLive() throws Exception {
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
