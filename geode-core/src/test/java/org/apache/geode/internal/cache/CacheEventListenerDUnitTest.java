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
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.cache.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedTestRule;
import org.apache.geode.test.dunit.rules.SharedCountersRule;
import org.apache.geode.test.dunit.rules.SharedErrorCollector;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Converted from JUnit 3.
 *
 * @since 2.0
 */
@Category(DistributedTest.class)
@SuppressWarnings("serial")
public class CacheEventListenerDUnitTest implements Serializable {

  private static final Logger logger = LogService.getLogger();

  private static final String REGION_NAME = CacheEventListenerDUnitTest.class.getSimpleName();

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
        logger.info("Invoking afterCreate on listener; name={}", event.getKey());
        sharedCountersRule.increment(CREATES);

        errorCollector.checkThat(event.getDistributedMember(), equalTo(event.getCallbackArgument()));
        errorCollector.checkThat(event.getOperation(), equalTo(Operation.CREATE));
        errorCollector.checkThat(event.getNewValue(), equalTo(0));
        errorCollector.checkThat(event.getOldValue(), nullValue());

        if (event.getSerializedOldValue() != null) {
          errorCollector.checkThat(event.getSerializedOldValue().getDeserializedValue(), equalTo(event.getOldValue()));
        }
        if (event.getSerializedNewValue() != null) {
          errorCollector.checkThat(event.getSerializedNewValue().getDeserializedValue(), equalTo(event.getNewValue()));
        }

        logger.info("create event new value is: {}", event.getNewValue());
      }

      @Override
      public void afterUpdate(EntryEvent event) {
        logger.info("Invoking afterUpdate on listener; name=" + event.getKey());
        sharedCountersRule.increment(UPDATES);
        newValue = event.getNewValue();
        oldValue = event.getOldValue();
        logger.info("update event new value is: " + newValue);
        logger.info("update event old value is: " + oldValue);

        errorCollector.checkThat(event.getDistributedMember(), equalTo(event.getCallbackArgument()));
        errorCollector.checkThat(event.getOperation(), equalTo(Operation.UPDATE));
        errorCollector.checkThat(event.getOldValue(), notNullValue());

        if (event.getSerializedOldValue() != null) {
          errorCollector.checkThat(event.getSerializedOldValue().getDeserializedValue(), equalTo(event.getOldValue()));
        }
        if (event.getSerializedNewValue() != null) {
          errorCollector.checkThat(event.getSerializedNewValue().getDeserializedValue(), equalTo(event.getNewValue()));
        }

      }
    };

    int expectedCreates = 1;
    int expectedUpdates = 0;

    // Create in controller VM
    createEntry(name, listener);

    assertThat(sharedCountersRule.getTotal(CREATES)).isEqualTo(expectedCreates);
    assertThat(sharedCountersRule.getTotal(UPDATES)).isEqualTo(expectedUpdates);

    // Create in other DUnit VMs

    // the expected number of update events is dependent on the number of VMs.
    // Each "createEntry" does a put which fires a update event in each of
    // the other VMs that have that entry already defined at that time.
    // The first createEntry causes 0 update events.
    // The second createEntry causes 1 update events (in the first VM)
    // The third createEntry causes 2 update events, etc (in the first 2 VMs)
    for (int i = 0; i < getHost(0).getVMCount(); i++) {
      getHost(0).getVM(i).invoke(() -> createEntry(name, listener));

      expectedCreates++;
      expectedUpdates += (i + 1);
    }

    assertThat(sharedCountersRule.getTotal(CREATES)).isEqualTo(expectedCreates);
    assertThat(sharedCountersRule.getTotal(UPDATES)).isEqualTo(expectedUpdates);

    assertThat(newValue).isEqualTo(0);
    assertThat(oldValue).isEqualTo(0);

    // test replace events
    int replaceValue = 1;
    replaceEntry(name, replaceValue);
    expectedUpdates += getHost(0).getVMCount() + 1;

    assertThat(sharedCountersRule.getTotal(CREATES)).isEqualTo(expectedCreates);
    assertThat(sharedCountersRule.getTotal(UPDATES)).isEqualTo(expectedUpdates);

    assertThat(oldValue).isEqualTo(replaceValue - 1);
    assertThat(newValue).isEqualTo(replaceValue);

    for (int i = 0; i < getHost(0).getVMCount(); i++) {
      Object vmOldValue = getHost(0).getVM(i).invoke(() -> oldValue);
      Object vmNewValue = getHost(0).getVM(i).invoke(() -> newValue);
      assertThat(vmOldValue).as("oldValue is wrong in vm " + i).isEqualTo(replaceValue - 1);
      assertThat(vmNewValue).as("newValue is wrong in vm " + i).isEqualTo(replaceValue);
    }

    replaceValue++;
    int finalReplaceValue = replaceValue;
    getHost(0).getVM(0).invoke(() -> replaceEntry(name, finalReplaceValue));
    expectedUpdates += getHost(0).getVMCount() + 1;

    assertThat(sharedCountersRule.getTotal(CREATES)).isEqualTo(expectedCreates);
    assertThat(sharedCountersRule.getTotal(UPDATES)).isEqualTo(expectedUpdates);

    assertThat(oldValue).isEqualTo(replaceValue - 1);
    assertThat(newValue).isEqualTo(replaceValue);

    for (int i = 0; i < getHost(0).getVMCount(); i++) {
      Object vmOldValue = getHost(0).getVM(i).invoke(() -> oldValue);
      Object vmNewValue = getHost(0).getVM(i).invoke(() -> newValue);
      assertThat(vmOldValue).as("oldValue is wrong in vm " + i).isEqualTo(replaceValue - 1);
      assertThat(vmNewValue).as("newValue is wrong in vm " + i).isEqualTo(replaceValue);
    }
  }

  @Test
  public void testObjectInvalidated() throws Exception {
    int timeToLive = 5; // seconds

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
        errorCollector.checkThat(event.getOperation(), equalTo(Operation.EXPIRE_LOCAL_INVALIDATE));
      }

      @Override
      public void afterUpdate(EntryEvent event) {
        // ignore
      }
    };

    // Create in controller VM
    createEntry(name, timeToLive, ExpirationAction.LOCAL_INVALIDATE, listener);

    // Create in other DUnit VMs
    long start = System.currentTimeMillis();
    invokeInEveryVM(
        () -> createEntry(name, timeToLive, ExpirationAction.LOCAL_INVALIDATE, listener));

    long time = System.currentTimeMillis() - start;
    assertThat(sharedCountersRule.getTotal(INVALIDATES))
        .as("Test timing intolerance: entry creation took " + time
            + " ms. Increase expiration time in test.")
        .isEqualTo(0);

    // Wait for invalidation
    await().atMost(1, MINUTES)
        .until(() -> assertThat(sharedCountersRule.getTotal(INVALIDATES))
            .isGreaterThanOrEqualTo(getHost(0).getVMCount() + 1));

    assertThat(getRegion().getSubregion(name).get(name)).isNull();

    assertThat(newValue).isNull();
    assertThat(oldValue).isEqualTo(0);

    for (int i = 0; i < getHost(0).getVMCount(); i++) {
      Object vmOldValue = getHost(0).getVM(i).invoke(() -> oldValue);
      Object vmNewValue = getHost(0).getVM(i).invoke(() -> newValue);
      assertThat(vmOldValue).as("oldValue is wrong in vm " + i).isEqualTo(0);
      assertThat(vmNewValue).as("newValue is wrong in vm " + i).isNull();
    }
  }

  @Test
  public void testObjectDestroyed() throws Exception {
    int timeToLive = 3; // seconds

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
        errorCollector.checkThat(event.getOperation(), equalTo(Operation.EXPIRE_DESTROY));
      }
    };

    // Create in Master VM
    createEntry(name, timeToLive, ExpirationAction.DESTROY, listener);

    // Create in other VMs
    invokeInEveryVM(() -> createEntry(name, timeToLive, ExpirationAction.DESTROY, listener));

    // Wait for invalidation
    // invoked at least once
    // invoked no more than once
    await().atMost(1, MINUTES)
        .until(() -> assertThat(sharedCountersRule.getTotal(DESTROYS))
            .isGreaterThanOrEqualTo(getHost(0).getVMCount() + 1));

    assertThat(newValue).isNull();
    assertThat(oldValue).isEqualTo(0);

    for (int i = 0; i < getHost(0).getVMCount(); i++) {
      Object vmOldValue = getHost(0).getVM(i).invoke(() -> oldValue);
      Object vmNewValue = getHost(0).getVM(i).invoke(() -> newValue);
      assertThat(vmOldValue).as("oldValue is wrong in vm " + i).isEqualTo(0);
      assertThat(vmNewValue).as("newValue is wrong in vm " + i).isNull();
    }
  }

  @Test
  public void testInvalidatedViaExpiration() throws Exception {
    int timeToLive = 6; // seconds

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
      public void afterInvalidate(EntryEvent event) {
        Object key = event.getKey();
        boolean isPresentLocally = event.getRegion().getEntry(key).getValue() != null;
        logger.info("Invoking test's invalidate listener (via expiration) isPresentLocally=" + isPresentLocally + "; newValue=" + event.getNewValue());
        sharedCountersRule.increment(INVALIDATES);
        newValue = event.getNewValue();
        oldValue = event.getOldValue();

        errorCollector.checkThat(event.isOriginRemote(), equalTo(false));
        errorCollector.checkThat(event.getDistributedMember(), equalTo(cacheRule.getSystem().getDistributedMember()));
        errorCollector.checkThat(event.getOperation(), equalTo(Operation.EXPIRE_LOCAL_INVALIDATE));
      }
    };

    // Create in controller VM
    createEntry(name, timeToLive, ExpirationAction.LOCAL_INVALIDATE, listener);

    // Create in other VMs
    invokeInEveryVM(
        () -> createEntry(name, timeToLive, ExpirationAction.LOCAL_INVALIDATE, listener));

    // Wait for invalidation
    // invoked at least once
    // invoked no more than once
    await().atMost(1, MINUTES)
        .until(() -> assertThat(sharedCountersRule.getTotal(INVALIDATES)).isEqualTo(getHost(0).getVMCount() + 1));

    // not present locally
    Region region = getRegion().getSubregion(name);
    Region.Entry entry = region.getEntry(name);
    assertThat(entry).isNotNull();
    assertThat(entry.getValue()).isNull();

    assertThat(oldValue).isEqualTo(0);
    assertThat(newValue).isNull();

    for (int i = 0; i < getHost(0).getVMCount(); i++) {
      Object vmOldValue = getHost(0).getVM(i).invoke(() -> oldValue);
      Object vmNewValue = getHost(0).getVM(i).invoke(() -> newValue);
      assertThat(vmOldValue).as("oldValue is wrong in vm " + i).isEqualTo(0);
      assertThat(vmNewValue).as("newValue is wrong in vm " + i).isNull();
    }
  }

  /**
   * Create a region with one entry in this test's region with the given name and attributes.
   */
  private void createEntry(String name, int timeToLive, ExpirationAction expirationAction,
      CacheEventListener listener) {
    Region region = getRegion();

    AttributesFactory factory = new AttributesFactory(region.getAttributes());
    factory.setStatisticsEnabled(true);
    factory.setEntryTimeToLive(new ExpirationAttributes(timeToLive, expirationAction));
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setCacheListener(listener);

    Region subregion = region.createSubregion(name, factory.create());
    subregion.create(name, 0, subregion.getCache().getDistributedSystem().getDistributedMember());
  }

  /**
   * Create an entry in this test's region with the given name
   */
  private void createEntry(String name, CacheEventListener listener) {
    createEntry(name, 0, ExpirationAction.INVALIDATE, listener);
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
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      subregion = region.createSubregion(REGION_NAME, factory.create());
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
      fail("Unexpected listener callback: afterInvalidate");
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
