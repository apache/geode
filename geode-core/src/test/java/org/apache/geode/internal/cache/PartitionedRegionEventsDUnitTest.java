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
package org.apache.geode.internal.cache;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.io.Serializable;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.cache.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedTestRule;
import org.apache.geode.test.dunit.rules.SharedCountersRule;
import org.apache.geode.test.dunit.rules.SharedErrorCollector;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * This class tests event triggering and handling in partitioned regions. Most of this class is a
 * copy of {@link CacheEventListenerDUnitTest}.
 *
 * <p>
 * Converted from JUnit 3.
 *
 * @since 5.1
 */
@Category(DistributedTest.class)
@SuppressWarnings("serial")
public class PartitionedRegionEventsDUnitTest implements Serializable {

  private static final Logger logger = LogService.getLogger();

  private static final String REGION_NAME = PartitionedRegionEventsDUnitTest.class.getSimpleName();

  private static final String CREATES = "CREATES";
  private static final String UPDATES = "UPDATES";
  private static final String INVALIDATES = "INVALIDATES";
  private static final String DESTROYS = "DESTROYS";

  private static volatile Object newValue;
  private static volatile Object oldValue;

  private String name;

  @ClassRule
  public static DistributedTestRule distributedTestRule = new DistributedTestRule();

  @Rule
  public CacheRule cacheRule = CacheRule.builder().createCacheInAll().build();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public SharedCountersRule sharedCountersRule = SharedCountersRule.builder().build();

  @Rule
  public SharedErrorCollector errorCollector = new SharedErrorCollector();

  @Before
  public void setUp() throws Exception {
    name = testName.getMethodName();

    sharedCountersRule.initialize(CREATES);
    sharedCountersRule.initialize(DESTROYS);
    sharedCountersRule.initialize(INVALIDATES);
    sharedCountersRule.initialize(UPDATES);

    cacheRule.getCache().createRegionFactory().setScope(Scope.DISTRIBUTED_ACK).create("root");
    invokeInEveryVM(() -> {
      cacheRule.getCache().createRegionFactory().setScope(Scope.DISTRIBUTED_ACK).create("root");
    });
  }

  @After
  public void tearDown() throws Exception {
    newValue = null;
    oldValue = null;
    invokeInEveryVM(() -> {
      newValue = null;
      oldValue = null;
    });
  }

  @Test
  public void testObjectAddedReplaced() throws Exception {
    CacheEventListener listener = new CacheEventListener() {

      @Override
      public void afterCreate(EntryEvent event) {
        logger.info("Invoking afterCreate on listener; event={}", event);
        sharedCountersRule.increment(CREATES);

        errorCollector.checkThat(event.getDistributedMember(), equalTo(event.getCallbackArgument()));
        errorCollector.checkThat(event.getOperation(), equalTo(Operation.CREATE));
        errorCollector.checkThat(event.getNewValue(), equalTo(0));
        errorCollector.checkThat(event.getOldValue(), nullValue());
      }

      @Override
      public void afterUpdate(EntryEvent event) {
        logger.info("Invoking afterUpdate on listener; name={}", event.getKey());
        sharedCountersRule.increment(UPDATES);
        newValue = event.getNewValue();
        oldValue = event.getOldValue();
        logger.info("update event new value is: {}", newValue);
        logger.info("update event old value is: {}", oldValue);

        errorCollector.checkThat(event.getDistributedMember(), equalTo(event.getCallbackArgument()));
        errorCollector.checkThat(event.getOperation(), equalTo(Operation.UPDATE));
        if (((EntryEventImpl) event).getInvokePRCallbacks()) {
          errorCollector.checkThat(event.getOldValue(), notNullValue());
        }
      }
    };

    int expectedCreates = 1;
    int expectedUpdates = 0;

    // Create in controller VM
    createEntry(name, new SubscriptionAttributes(InterestPolicy.ALL), listener);

    assertThat(sharedCountersRule.getTotal(CREATES)).isEqualTo(expectedCreates);
    assertThat(sharedCountersRule.getTotal(UPDATES)).isEqualTo(expectedUpdates);

    // Create in other VMs

    // the expected number of update events is dependent on the number of VMs.
    // Each "createEntry" does a put which fires a update event in each of
    // the other VMs that have that entry already defined at that time.
    // The first createEntry causes 0 update events.
    // The second createEntry causes 1 update events (in the first VM)
    // The third createEntry causes 2 update events, etc (in the first 2 VMs)

    for (int i = 0; i < getHost(0).getVMCount(); i++) {
      getHost(0).getVM(i).invoke(
          () -> updateEntry(name, new SubscriptionAttributes(InterestPolicy.ALL), listener));
      expectedUpdates += (i + 2); // with InterestPolicy.ALL, all listeners should be invoked
    }

    assertThat(sharedCountersRule.getTotal(CREATES)).isEqualTo(expectedCreates);
    assertThat(sharedCountersRule.getTotal(UPDATES)).isEqualTo(expectedUpdates);

    assertThat(newValue).isEqualTo(0);

    // test replace events
    int replaceValue = 1;
    replaceEntry(name, replaceValue);
    expectedUpdates += getHost(0).getVMCount() + 1;

    assertThat(sharedCountersRule.getTotal(CREATES)).isEqualTo(expectedCreates);
    assertThat(sharedCountersRule.getTotal(UPDATES)).isEqualTo(expectedUpdates);

    assertThat(newValue).isEqualTo(replaceValue);

    for (int i = 0; i < getHost(0).getVMCount(); i++) {
      Object vmNewValue = getHost(0).getVM(i).invoke(() -> newValue);
      assertThat(vmNewValue).as("newValue is wrong in vm " + i).isEqualTo(replaceValue);
    }

    replaceValue++;
    int finalReplaceValue = replaceValue;
    getHost(0).getVM(0).invoke(() -> replaceEntry(name, finalReplaceValue));
    expectedUpdates += getHost(0).getVMCount() + 1;

    assertThat(sharedCountersRule.getTotal(CREATES)).isEqualTo(expectedCreates);
    assertThat(sharedCountersRule.getTotal(UPDATES)).isEqualTo(expectedUpdates);

    assertThat(newValue).isEqualTo(replaceValue);

    for (int i = 0; i < getHost(0).getVMCount(); i++) {
      Object vmNewValue = getHost(0).getVM(i).invoke(() -> newValue);
      assertThat(vmNewValue).as("newValue is wrong in vm " + i).isEqualTo(replaceValue);
    }
  }

  @Test
  public void testUpdateIsCreate() throws Exception {
    CacheEventListener listener = new CacheEventListener() {

      @Override
      public void afterCreate(EntryEvent event) {
        logger.info("Invoking afterCreate on listener; event={}", event);
        sharedCountersRule.increment(CREATES);

        errorCollector.checkThat(event.getDistributedMember(), equalTo(event.getCallbackArgument()));
        errorCollector.checkThat(event.getOperation(), equalTo(Operation.CREATE));
        if (!((EntryEventImpl) event).getInvokePRCallbacks()) {
          errorCollector.checkThat(event.getOldValue(), nullValue());
          errorCollector.checkThat(event.isOldValueAvailable(), equalTo(false));
        } else {
          errorCollector.checkThat(event.getOldValue(), nullValue());
        }
        errorCollector.checkThat(event.getNewValue(), equalTo(0));
      }

      @Override
      public void afterUpdate(EntryEvent event) {
        logger.info("Invoking afterUpdate on listener; name={}", event.getKey());
        sharedCountersRule.increment(UPDATES);

        errorCollector.checkThat(event.getOperation(), equalTo(Operation.UPDATE));
      }
    };

    // Create in controller vm
    createRegion(name, new SubscriptionAttributes(InterestPolicy.ALL), listener);

    // Create in dunit VMs
    for (int i = 1; i < getHost(0).getVMCount(); i++) {
      getHost(0).getVM(i).invoke(
          () -> createRegion(name, new SubscriptionAttributes(InterestPolicy.ALL), listener));
    }

    int expectedCreates = getHost(0).getVMCount() + 1;
    int expectedUpdates = 0;

    getHost(0).getVM(0).invoke(
        () -> updateNewEntry(name, new SubscriptionAttributes(InterestPolicy.ALL), listener));

    assertThat(sharedCountersRule.getTotal(UPDATES)).isEqualTo(expectedUpdates);
    assertThat(sharedCountersRule.getTotal(CREATES)).isEqualTo(expectedCreates);
  }

  @Test
  public void testObjectInvalidated() throws Exception {
    int timeToLive = 0; // 5; // seconds expiration isn't supported in PR (yet??)

    CacheEventListener listener = new CacheEventListener() {

      @Override
      public void afterCreate(EntryEvent event) {
        // ignore
      }

      @Override
      public void afterInvalidate(EntryEvent event) {
        logger.info("Invoking tests invalidated listener");
        sharedCountersRule.increment(INVALIDATES);
        newValue = event.getNewValue();
        oldValue = event.getOldValue();

        if (event.isOriginRemote()) {
          errorCollector.checkThat(event.getDistributedMember(), not(cacheRule.getSystem().getDistributedMember()));
        } else {
          errorCollector.checkThat(event.getDistributedMember(), equalTo(cacheRule.getSystem().getDistributedMember()));
        }
        errorCollector.checkThat(event.getOperation(), equalTo(Operation.INVALIDATE));
      }

      @Override
      public void afterUpdate(EntryEvent event) {
        // ignore
      }
    };

    // Create in controller VM
    createEntry(name, timeToLive, ExpirationAction.INVALIDATE,
        new SubscriptionAttributes(InterestPolicy.ALL), listener);

    // create region and listeners in other VMs
    invokeInEveryVM(
        () -> createSubregion(name, new SubscriptionAttributes(InterestPolicy.ALL), listener));

    // Wait for expiration

    // Invalidate the entry
    Region region = getRegion();
    Region subregion = region.getSubregion(name);
    subregion.invalidate(name);

    // invoked at least once
    // invoked no more than once
    await().atMost(1, MINUTES).until(
        () -> assertThat(sharedCountersRule.getTotal(INVALIDATES)).isGreaterThanOrEqualTo(
            getHost(0).getVMCount() + 1));

    assertThat(getRegion().getSubregion(name).get(name)).isNull();

    assertThat(newValue).isNull();

    for (int i = 0; i < getHost(0).getVMCount(); i++) {
      Object vmNewValue = getHost(0).getVM(i).invoke(() -> newValue);
      assertThat(vmNewValue).as("newValue is wrong in vm " + i).isNull();
    }
  }

  @Test
  public void testObjectDestroyed() throws Exception {
    int timeToLive = 0; // 3; expiration isn't supported by PRs (yet???)

    CacheEventListener listener = new CacheEventListener() {

      @Override
      public void afterCreate(EntryEvent event) {
        // ignore
      }

      @Override
      public void afterUpdate(EntryEvent event) {
        // ignore
      }

      @Override
      public void afterDestroy(EntryEvent event) {
        logger.info("Invoking objectDestroyed listener");
        sharedCountersRule.increment(DESTROYS);
        newValue = event.getNewValue();
        oldValue = event.getOldValue();

        if (event.isOriginRemote()) {
          errorCollector.checkThat(event.getDistributedMember(), not(cacheRule.getSystem().getDistributedMember()));
        } else {
          errorCollector.checkThat(event.getDistributedMember(), equalTo(cacheRule.getSystem().getDistributedMember()));
        }
        errorCollector.checkThat(event.getOperation().isDestroy(), equalTo(true));
      }
    };

    // Create in Master VM
    createEntry(name, timeToLive, ExpirationAction.DESTROY,
        new SubscriptionAttributes(InterestPolicy.ALL), listener);

    // create region and listeners in other VMs
    invokeInEveryVM(
        () -> createSubregionWhenDestroy(name, new SubscriptionAttributes(InterestPolicy.ALL),
            listener, timeToLive, ExpirationAction.DESTROY));

    // Wait for expiration

    // destroy entry
    Region region = getRegion();
    Region subregion = region.getSubregion(name);
    subregion.destroy(name);

    // invoked at least once
    // invoked no more than once
    await().atMost(1, MINUTES).until(
        () -> assertThat(sharedCountersRule.getTotal(DESTROYS)).isGreaterThanOrEqualTo(
            getHost(0).getVMCount() + 1));

    assertThat(newValue).isNull();

    for (int i = 0; i < getHost(0).getVMCount(); i++) {
      Object vmNewValue = getHost(0).getVM(i).invoke(() -> newValue);
      assertThat(vmNewValue).as("newValue is wrong in vm " + i).isNull();
    }
  }

  @Test
  public void testObjectAddedReplacedCACHECONTENT() throws Exception {
    CacheEventListener listener = new CacheEventListener() {

      @Override
      public void afterCreate(EntryEvent event) {
        logger.info("Invoking afterCreate on listener; name={}", event.getKey());
        sharedCountersRule.increment(CREATES);

        errorCollector.checkThat(event.getDistributedMember(), equalTo(event.getCallbackArgument()));
        errorCollector.checkThat(event.getOperation(), equalTo(Operation.CREATE));
        errorCollector.checkThat(event.getOldValue(), nullValue());
        errorCollector.checkThat(event.getNewValue(), equalTo(0));
        errorCollector.checkThat(event.getOldValue(), nullValue());
      }

      @Override
      public void afterUpdate(EntryEvent event) {
        logger.info("Invoking afterUpdate on listener; name={}", event.getKey());
        sharedCountersRule.increment(UPDATES);
        newValue = event.getNewValue();
        oldValue = event.getOldValue();
        logger.info("update event new value is: {}", newValue);
        logger.info("update event old value is: {}", oldValue);

        errorCollector.checkThat(event.getDistributedMember(), equalTo(event.getCallbackArgument()));
        errorCollector.checkThat(event.getOperation(), equalTo(Operation.UPDATE));
        errorCollector.checkThat(event.getOldValue(), notNullValue());
      }
    };

    int expectedCreates = 1;
    int expectedUpdates = 0;

    // Create in controller VM
    createEntry(name, new SubscriptionAttributes(InterestPolicy.CACHE_CONTENT), listener);

    assertThat(sharedCountersRule.getTotal(CREATES)).isEqualTo(expectedCreates);
    assertThat(sharedCountersRule.getTotal(UPDATES)).isEqualTo(expectedUpdates);

    // Create in other VMs

    // the expected number of update events is dependent on the number of VMs.
    // Each "createEntry" does a put which fires a update event in each of
    // the other VMs that have that entry already defined at that time.
    // The first createEntry causes 0 update events.
    // The second createEntry causes 1 update events (in the first VM)
    // The third createEntry causes 2 update events, etc (in the first 2 VMs)

    for (int i = 0; i < getHost(0).getVMCount(); i++) {
      getHost(0).getVM(i).invoke(() -> updateEntry(name,
          new SubscriptionAttributes(InterestPolicy.CACHE_CONTENT), listener));
      expectedUpdates += 1;
    }

    assertThat(sharedCountersRule.getTotal(CREATES)).isEqualTo(expectedCreates);
    assertThat(sharedCountersRule.getTotal(UPDATES)).isEqualTo(expectedUpdates);

    assertThat(newValue).isEqualTo(0);

    // test replace events
    int replaceValue = 1;
    replaceEntry(name, replaceValue);
    expectedUpdates += 1;

    assertThat(sharedCountersRule.getTotal(CREATES)).isEqualTo(expectedCreates);
    assertThat(sharedCountersRule.getTotal(UPDATES)).isEqualTo(expectedUpdates);

    Region region = getRegion();
    Region subregion = region.getSubregion(name);

    assertThat(newValue).isEqualTo(replaceValue);

    // find the vm that should have the bucket and see if it has the correct new value
    DistributedMember bucketOwner = ((PartitionedRegion) subregion).getMemberOwning(name);
    assertThat(bucketOwner).isNotNull();

    for (int i = 0; i < getHost(0).getVMCount(); i++) {
      Object id = getHost(0).getVM(i).invoke(() -> cacheRule.getSystem().getDistributedMember());
      assertThat(bucketOwner).isNotEqualTo(id);
      Object vmNewValue = getHost(0).getVM(i).invoke(() -> newValue);
      assertThat(vmNewValue).as("newValue is wrong in vm " + i).isNull();
    }

    replaceValue++;
    int finalReplaceValue = replaceValue;
    getHost(0).getVM(0).invoke(() -> replaceEntry(name, finalReplaceValue));
    expectedUpdates += 1;

    assertThat(sharedCountersRule.getTotal(CREATES)).isEqualTo(expectedCreates);
    assertThat(sharedCountersRule.getTotal(UPDATES)).isEqualTo(expectedUpdates);

    bucketOwner = ((PartitionedRegion) subregion).getMemberOwning(name);
    assertThat(bucketOwner).isNotNull();

    for (int i = 0; i < getHost(0).getVMCount(); i++) {
      Object id = getHost(0).getVM(i).invoke(() -> cacheRule.getSystem().getDistributedMember());
      assertThat(bucketOwner).isNotEqualTo(id);
    }
  }

  @Test
  public void testObjectInvalidatedCACHECONTENT() throws Exception {
    int timeToLive = 0; // 5; // seconds expiration isn't supported in PR (yet??)

    CacheEventListener listener = new CacheEventListener() {

      @Override
      public void afterCreate(EntryEvent event) {
        // ignore
      }

      @Override
      public void afterInvalidate(EntryEvent event) {
        logger.info("Invoking tests invalidated listener");
        sharedCountersRule.increment(INVALIDATES);
        newValue = event.getNewValue();
        oldValue = event.getOldValue();

        if (event.isOriginRemote()) {
          errorCollector.checkThat(event.getDistributedMember(), not(cacheRule.getSystem().getDistributedMember()));
        } else {
          errorCollector.checkThat(event.getDistributedMember(), equalTo(cacheRule.getSystem().getDistributedMember()));
        }
        errorCollector.checkThat(event.getOperation(), equalTo(Operation.INVALIDATE));
      }

      @Override
      public void afterUpdate(EntryEvent event) {
        // ignore
      }
    };

    // Create in controller VM
    createEntry(name, timeToLive, ExpirationAction.INVALIDATE,
        new SubscriptionAttributes(InterestPolicy.CACHE_CONTENT), listener);

    // create region and listeners in other VMs
    invokeInEveryVM(() -> createSubregion(name,
        new SubscriptionAttributes(InterestPolicy.CACHE_CONTENT), listener));

    // Wait for expiration

    // Invalidate the entry
    Region region = getRegion();
    Region subregion = region.getSubregion(name);
    subregion.invalidate(name);

    // invoked at least once
    // invoked no more than once
    await().atMost(1, MINUTES)
        .until(() -> assertThat(sharedCountersRule.getTotal(INVALIDATES)).isEqualTo(1));

    assertThat(getRegion().getSubregion(name).get(name)).isNull();

    DistributedMember bucketOwner = ((PartitionedRegion) subregion).getMemberOwning(name);
    assertThat(bucketOwner).isNotNull();

    for (int i = 0; i < getHost(0).getVMCount(); i++) {
      Object id = getHost(0).getVM(i).invoke(() -> cacheRule.getSystem().getDistributedMember());
      assertThat(bucketOwner).isNotEqualTo(id);
    }
  }

  @Test
  public void testObjectDestroyedCACHECONTENT() throws Exception {
    int timeToLive = 0; // 3; expiration isn't supported by PRs (yet???)

    CacheEventListener listener = new CacheEventListener() {

      @Override
      public void afterCreate(EntryEvent event) {
        // ignore
      }

      @Override
      public void afterUpdate(EntryEvent event) {
        // ignore
      }

      @Override
      public void afterDestroy(EntryEvent event) {
        logger.info("Invoking objectDestroyed listener");
        sharedCountersRule.increment(DESTROYS);
        newValue = event.getNewValue();
        oldValue = event.getOldValue();

        if (event.isOriginRemote()) {
          errorCollector.checkThat(event.getDistributedMember(), not(cacheRule.getSystem().getDistributedMember()));
        } else {
          errorCollector.checkThat(event.getDistributedMember(), equalTo(cacheRule.getSystem().getDistributedMember()));
        }
        errorCollector.checkThat(event.getOperation().isDestroy(), equalTo(true));
      }
    };

    // Create in Master VM
    createEntry(name, timeToLive, ExpirationAction.DESTROY,
        new SubscriptionAttributes(InterestPolicy.CACHE_CONTENT), listener);

    // create region and listeners in other VMs
    invokeInEveryVM(() -> createSubregionWhenDestroy(name,
        new SubscriptionAttributes(InterestPolicy.CACHE_CONTENT), listener, timeToLive,
        ExpirationAction.DESTROY));

    // Wait for expiration

    // destroy the entry
    Region region = getRegion();
    Region subregion = region.getSubregion(name);
    subregion.destroy(name);

    // invoked at least once
    // invoked no more than once
    await().atMost(1, MINUTES)
        .until(() -> assertThat(sharedCountersRule.getTotal(DESTROYS)).isEqualTo(1));

    DistributedMember bucketOwner = ((PartitionedRegion) subregion).getMemberOwning(name);
    assertThat(bucketOwner).isNotNull();

    for (int i = 0; i < getHost(0).getVMCount(); i++) {
      Object id = getHost(0).getVM(i).invoke(() -> cacheRule.getSystem().getDistributedMember());
      assertThat(bucketOwner).isNotEqualTo(id);
    }
  }

  /**
   * Create a region and install the given listener
   */
  private void createSubregion(String name, SubscriptionAttributes subscriptionAttributes,
      CacheEventListener listener) {
    AttributesFactory factory = new AttributesFactory();
    factory.setStatisticsEnabled(true);
    factory.setSubscriptionAttributes(subscriptionAttributes);
    factory.addCacheListener(listener);
    factory.setPartitionAttributes((new PartitionAttributesFactory()).create());

    Region region = getRegion();
    region.createSubregion(name, factory.create());
  }

  /**
   * Create a region and install the given listener
   */
  private void createSubregionWhenDestroy(String name,
      SubscriptionAttributes subscriptionAttributes, CacheEventListener listener, int timeToLive,
      ExpirationAction expirationAction) {
    AttributesFactory factory = new AttributesFactory();
    factory.setStatisticsEnabled(true);
    factory.setSubscriptionAttributes(subscriptionAttributes);
    factory.setEntryTimeToLive(new ExpirationAttributes(timeToLive, expirationAction));
    factory.addCacheListener(listener);
    factory.setPartitionAttributes((new PartitionAttributesFactory()).create());

    Region region = getRegion();
    region.createSubregion(name, factory.create());
  }

  /**
   * Create a region with one entry in this test's region with the given name and attributes.
   */
  private void createEntry(String name, int timeToLive, ExpirationAction expirationAction,
      SubscriptionAttributes subscriptionAttributes, CacheEventListener listener) {
    Region subregion =
        createRegion(name, timeToLive, expirationAction, subscriptionAttributes, listener);
    subregion.create(name, 0, subregion.getCache().getDistributedSystem().getDistributedMember());
  }

  private Region createRegion(String name, int timeToLive, ExpirationAction expirationAction,
      SubscriptionAttributes subscriptionAttributes, CacheEventListener listener) {
    AttributesFactory factory = new AttributesFactory();
    factory.setStatisticsEnabled(true);
    factory.setEntryTimeToLive(new ExpirationAttributes(timeToLive, expirationAction));
    factory.setSubscriptionAttributes(subscriptionAttributes);
    factory.addCacheListener(listener);
    factory.setPartitionAttributes((new PartitionAttributesFactory()).create());

    Region region = getRegion();
    return region.createSubregion(name, factory.create());
  }

  /**
   * Create an entry in this test's region with the given name
   */
  private void createEntry(String name, SubscriptionAttributes subscriptionAttributes,
      CacheEventListener listener) {
    createEntry(name, 0, ExpirationAction.INVALIDATE, subscriptionAttributes, listener);
  }

  /**
   * Create an entry in this test's region with the given name
   */
  private void createRegion(String name, SubscriptionAttributes subscriptionAttributes,
      CacheEventListener listener) {
    createRegion(name, 0, ExpirationAction.INVALIDATE, subscriptionAttributes, listener);
  }

  /**
   * Create a region and update one entry in this test's region with the given name and attributes.
   */
  private void updateEntry(String name, int timeToLive, ExpirationAction expirationAction,
      SubscriptionAttributes subscriptionAttributes, CacheEventListener listener) {
    Region subregion =
        createRegion(name, timeToLive, expirationAction, subscriptionAttributes, listener);
    subregion.put(name, 0, subregion.getCache().getDistributedSystem().getDistributedMember());
  }

  /**
   * Update an entry in this test's region with the given name
   */
  private void updateEntry(String name, SubscriptionAttributes subscriptionAttributes,
      CacheEventListener listener) {
    updateEntry(name, 0, ExpirationAction.INVALIDATE, subscriptionAttributes, listener);
  }

  /**
   * Update an entry in this test's region with the given name, assuming that the update is actually
   * causing creation of the entry
   */
  private void updateNewEntry(String name, SubscriptionAttributes subscriptionAttributes,
      CacheEventListener listener) {
    Region subregion =
        createRegion(name, 0, ExpirationAction.INVALIDATE, subscriptionAttributes, listener);
    subregion.put(name, 0, subregion.getCache().getDistributedSystem().getDistributedMember());
  }

  private void replaceEntry(String name, Object value) {
    Region region = getRegion();
    Region subregion = region.getSubregion(name);
    subregion.put(name, value, subregion.getCache().getDistributedSystem().getDistributedMember());
  }

  /**
   * Gets or creates a region used in this test
   */
  private Region getRegion() {
    Region region = cacheRule.getCache().getRegion("root");
    Region subregion = region.getSubregion(REGION_NAME);
    if (subregion == null) {
      subregion = region.createSubregion(REGION_NAME, new AttributesFactory().create());
    }
    return subregion;
  }

  /**
   * Overridden within tests to increment shared counters.
   */
  public static class CacheEventListener extends CacheListenerAdapter implements Serializable {

    @Override
    public void close() {
      // nothing
    }

    @Override
    public void afterCreate(EntryEvent event) {
      fail("Unexpected listener callback: afterCreate");
    }

    @Override
    public void afterInvalidate(EntryEvent event) {
      fail("Unexpected listener callback: afterInvalidated");
    }

    @Override
    public void afterDestroy(EntryEvent event) {
      fail("Unexpected listener callback: afterDestroy");
    }

    @Override
    public void afterUpdate(EntryEvent event) {
      fail("Unexpected listener callback: afterUpdate");
    }

    @Override
    public void afterRegionInvalidate(RegionEvent event) {
      fail("Unexpected listener callback: afterRegionInvalidate");
    }

    @Override
    public void afterRegionDestroy(RegionEvent event) {
      // nothing
    }
  }
}
