/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache30;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import dunit.*;
/**
 * Test create + localDestroy for bug 34387
 *
 * @author darrel
 * @since 5.0
 */
public class Bug34387DUnitTest extends CacheTestCase {

//  private transient Region r;
//  private transient DistributedMember otherId;
  protected transient int invokeCount;
  
  static volatile boolean callbackFailure;
  
  public Bug34387DUnitTest(String name) {
    super(name);
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
    vm.invoke(Bug34387DUnitTest.class, "getVMDistributedMember");
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
