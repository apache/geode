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
package org.apache.geode.cache30;

import static org.apache.geode.internal.i18n.LocalizedStrings.TXStateStub_LOCAL_DESTROY_NOT_ALLOWED_IN_TRANSACTION;
import static org.apache.geode.internal.i18n.LocalizedStrings.TXStateStub_LOCAL_INVALIDATE_NOT_ALLOWED_IN_TRANSACTION;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.UnsupportedOperationInTransactionException;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.serializable.SerializableErrorCollector;

/**
 * Test create + localDestroy for bug 34387
 *
 * <p>
 * TRAC #34387: TX in Proxy Regions with create followed by localDestroy on same key results in
 * remote VMs receiving create events with null getNewValue().
 *
 * <p>
 * Create and LocalDestroy/LocalInvalidate should create event with NewValue
 *
 * @since GemFire 5.0
 */
@Category(DistributedTest.class)
public class CreateAndLocalDestroyInTXRegressionTest extends CacheTestCase {

  private static final String REGION_NAME = "r1";

  private int invokeCount;
  private VM otherVM;
  private transient Region region;

  @Rule
  public SerializableErrorCollector errorCollector = new SerializableErrorCollector();

  @Before
  public void setUp() throws Exception {
    this.invokeCount = 0;
    this.otherVM = Host.getHost(0).getVM(0);

    initOtherVM(this.otherVM);
    AttributesFactory af = new AttributesFactory();
    af.setDataPolicy(DataPolicy.REPLICATE);
    af.setScope(Scope.DISTRIBUTED_ACK);
    af.setConcurrencyChecksEnabled(true);

    CacheListener cl1 = new CacheListenerAdapter() {
      @Override
      public void afterCreate(EntryEvent e) {
        errorCollector.checkThat("Keys not equal", "createKey", equalTo(e.getKey()));
        errorCollector.checkThat("Values not equal", "createValue", equalTo(e.getNewValue()));
        CreateAndLocalDestroyInTXRegressionTest.this.invokeCount++;
      }
    };

    af.addCacheListener(cl1);
    this.region = createRootRegion(REGION_NAME, af.create());

    assertNull(this.region.getEntry("createKey"));
  }

  /**
   * test create followed by localDestroy
   */
  @Test
  public void createAndLocalDestroyShouldCreateEventWithNewValue() throws CacheException {
    doCommitInOtherVm(otherVM, true);

    assertNotNull(this.region.getEntry("createKey"));
    assertEquals("createValue", this.region.getEntry("createKey").getValue());
    assertEquals(1, this.invokeCount);
  }

  /**
   * test create followed by localInvalidate
   */
  @Test
  public void createAndLocalInvalidateShouldCreateEventWithNewValue() throws CacheException {
    doCommitInOtherVm(this.otherVM, false);

    assertNotNull(this.region.getEntry("createKey"));
    assertEquals("createValue", this.region.getEntry("createKey").getValue());
    assertEquals(1, this.invokeCount);
  }

  private void initOtherVM(VM otherVM) {
    otherVM.invoke(new CacheSerializableRunnable("Connect") {
      @Override
      public void run2() throws CacheException {
        getCache();
      }
    });
  }

  private void doCommitInOtherVm(VM otherVM, boolean doDestroy) {
    otherVM.invoke(new CacheSerializableRunnable("create root") {
      @Override
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setConcurrencyChecksEnabled(true);

        Region region = createRootRegion(REGION_NAME, factory.create());

        CacheTransactionManager transactionManager = getCache().getCacheTransactionManager();
        transactionManager.begin();

        region.create("createKey", "createValue");

        if (doDestroy) {
          try {
            region.localDestroy("createKey");
            fail("expected exception not thrown");
          } catch (UnsupportedOperationInTransactionException e) {
            assertEquals(TXStateStub_LOCAL_DESTROY_NOT_ALLOWED_IN_TRANSACTION.toLocalizedString(),
                e.getMessage());
          }
        } else {
          try {
            region.localInvalidate("createKey");
            fail("expected exception not thrown");
          } catch (UnsupportedOperationInTransactionException e) {
            assertEquals(
                TXStateStub_LOCAL_INVALIDATE_NOT_ALLOWED_IN_TRANSACTION.toLocalizedString(),
                e.getMessage());
          }
        }

        transactionManager.commit();
      }
    });
  }

}
