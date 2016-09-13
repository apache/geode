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
package org.apache.geode.cache30;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

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
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;

/**
 * Test create + localDestroy for bug 34387
 *
 * @since GemFire 5.0
 */
@Category(DistributedTest.class)
public class Bug34387DUnitTest extends JUnit4CacheTestCase {

//  private transient Region r;
//  private transient DistributedMember otherId;
  protected transient int invokeCount;
  
  static volatile boolean callbackFailure;
  
  public Bug34387DUnitTest() {
    super();
  }

  protected static void callbackAssertEquals(String message, Object expected, 
      Object actual) {
    if (expected == null && actual == null)
      return;
    if (expected != null && expected.equals(actual))
      return;
    callbackFailure = true;
    // Throws an error that is ignored, but...
    assertEquals(message, expected, actual);
  }
  

  private VM getOtherVm() {
    Host host = Host.getHost(0);
    return host.getVM(0);
  }
    
  private void initOtherId() {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("Connect") {
        public void run2() throws CacheException {
          getCache();
        }
      });
    vm.invoke(() -> Bug34387DUnitTest.getVMDistributedMember());
  }
  private void doCommitOtherVm(final boolean doDestroy) {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("create root") {
        public void run2() throws CacheException {
          AttributesFactory af = new AttributesFactory();
          af.setScope(Scope.DISTRIBUTED_ACK);
          af.setConcurrencyChecksEnabled(true);
          Region r1 = createRootRegion("r1", af.create());
          CacheTransactionManager ctm =  getCache().getCacheTransactionManager();
          ctm.begin();
          r1.create("createKey", "createValue");
          if (doDestroy) {
            try {
              r1.localDestroy("createKey");
              fail("expected exception not thrown");
            } catch (UnsupportedOperationInTransactionException e) {
              assertEquals(e.getMessage(), LocalizedStrings.TXStateStub_LOCAL_DESTROY_NOT_ALLOWED_IN_TRANSACTION.toLocalizedString());
            }
          } else {
            try {
              r1.localInvalidate("createKey");
              fail("expected exception not thrown");
            } catch (UnsupportedOperationInTransactionException e) {
              assertEquals(e.getMessage(), LocalizedStrings.TXStateStub_LOCAL_INVALIDATE_NOT_ALLOWED_IN_TRANSACTION.toLocalizedString());
            }
          }
          ctm.commit();
        }
      });
  }

  public static DistributedMember getVMDistributedMember() {
    return InternalDistributedSystem.getAnyInstance().getDistributedMember();
  }
  
  //////////////////////  Test Methods  //////////////////////

  /**
   * test create followed by localDestroy
   */
  @Test
  public void testCreateAndLD() throws CacheException {
    initOtherId();
    AttributesFactory af = new AttributesFactory();
    af.setDataPolicy(DataPolicy.REPLICATE);
    af.setScope(Scope.DISTRIBUTED_ACK);
    af.setConcurrencyChecksEnabled(true);
    callbackFailure = false;
    
    CacheListener cl1 = new CacheListenerAdapter() {
        public void afterCreate(EntryEvent e) {
          callbackAssertEquals("Keys not equal", "createKey", e.getKey());
          callbackAssertEquals("Values not equal", "createValue", e.getNewValue());
          Bug34387DUnitTest.this.invokeCount++;
        }
      };
    af.addCacheListener(cl1);
    Region r1 = createRootRegion("r1", af.create());

    this.invokeCount = 0;
    assertNull(r1.getEntry("createKey"));
    doCommitOtherVm(true);
    assertNotNull(r1.getEntry("createKey"));
    assertEquals("createValue", r1.getEntry("createKey").getValue());
    assertEquals(1, this.invokeCount);
    assertFalse("Errors in callbacks; check logs for details", callbackFailure);
  }
  /**
   * test create followed by localInvalidate
   */
  @Test
  public void testCreateAndLI() throws CacheException {
    initOtherId();
    AttributesFactory af = new AttributesFactory();
    af.setDataPolicy(DataPolicy.REPLICATE);
    af.setScope(Scope.DISTRIBUTED_ACK);
    af.setConcurrencyChecksEnabled(true);
    callbackFailure = false;
    
    CacheListener cl1 = new CacheListenerAdapter() {
        public void afterCreate(EntryEvent e) {
          callbackAssertEquals("key not equal", "createKey", e.getKey());
          callbackAssertEquals("value not equal", "createValue", e.getNewValue());
          Bug34387DUnitTest.this.invokeCount++;
        }
      };
    af.addCacheListener(cl1);
    Region r1 = createRootRegion("r1", af.create());

    this.invokeCount = 0;
    assertNull(r1.getEntry("createKey"));
    doCommitOtherVm(false);
    assertNotNull(r1.getEntry("createKey"));
    assertEquals("createValue", r1.getEntry("createKey").getValue());
    assertEquals(1, this.invokeCount);
    assertFalse("Errors in callbacks; check logs for details", callbackFailure);
  }
}
