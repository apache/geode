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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.TransactionEvent;
import org.apache.geode.cache.TransactionListener;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.TransactionListenerAdapter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;

/**
 * Test the getCallbackArgument in light of bug 34075.
 *
 * @since GemFire 5.0
 */
@Category(DistributedTest.class)
public class CallbackArgDUnitTest extends JUnit4CacheTestCase {

//  private transient Region r;
//  private transient DistributedMember otherId;
  protected transient int invokeCount;
  protected static String callbackArg;
  
  public CallbackArgDUnitTest() {
    super();
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
    vm.invoke(() -> CallbackArgDUnitTest.getVMDistributedMember());
  }
  private void doCommitOtherVm() {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("create root") {
        public void run2() throws CacheException {
          AttributesFactory af = new AttributesFactory();
          CacheListener cl1 = new CacheListenerAdapter() {
              public void afterCreate(EntryEvent e) {
                assertEquals(callbackArg, e.getCallbackArgument());
              }
            };
          af.addCacheListener(cl1);
          af.setScope(Scope.DISTRIBUTED_ACK);
          Region r1 = createRootRegion("r1", af.create());
          Region r2 = r1.createSubregion("r2", af.create());
          Region r3 = r2.createSubregion("r3", af.create());
          CacheTransactionManager ctm =  getCache().getCacheTransactionManager();
          TransactionListener tl1 = new TransactionListenerAdapter() {
              public void afterCommit(TransactionEvent e) {
                assertEquals(6, e.getEvents().size());
                Iterator it = e.getEvents().iterator();
                while (it.hasNext()) {
                  EntryEvent ee = (EntryEvent)it.next();
                  assertEquals(callbackArg, ee.getCallbackArgument());
                  assertEquals(true, ee.isCallbackArgumentAvailable());
                }
              }
            };
          ctm.addListener(tl1);
          ctm.begin();
          r2.put("b", "value1", callbackArg);
          r3.put("c", "value2", callbackArg);
          r1.put("a", "value3", callbackArg);
          r1.put("a2", "value4", callbackArg);
          r3.put("c2", "value5", callbackArg);
          r2.put("b2", "value6", callbackArg);
          ctm.commit();
        }
      });
  }

  public static DistributedMember getVMDistributedMember() {
    return InternalDistributedSystem.getAnyInstance().getDistributedMember();
  }
  
  //////////////////////  Test Methods  //////////////////////

  List expectedKeys;
  int clCount = 0;

  Object getCurrentExpectedKey() {
    Object result = this.expectedKeys.get(this.clCount);
    this.clCount += 1;
    return result;
  }
  /**
   * make sure callback arg is NOT_AVAILABLE in all the places it should be
   */
  @Test
  public void testForNA_CA() throws CacheException {
    doTest();
  }
  @Test
  public void testForCA() throws Exception {
    callbackArg = "cbArg";
    getOtherVm().invoke(new SerializableCallable() {
      public Object call() throws Exception {
        callbackArg = "cbArg";
        return null;
      }
    });
    doTest();
  }
  private void doTest() throws CacheException {
    initOtherId();
    AttributesFactory af = new AttributesFactory();
    af.setDataPolicy(DataPolicy.REPLICATE);
    af.setScope(Scope.DISTRIBUTED_ACK);
    CacheListener cl1 = new CacheListenerAdapter() {
        public void afterCreate(EntryEvent e) {
          assertEquals(getCurrentExpectedKey(), e.getKey());
          assertEquals(callbackArg, e.getCallbackArgument());
          assertEquals(true, e.isCallbackArgumentAvailable());
        }
      };
    af.addCacheListener(cl1);
    Region r1 = createRootRegion("r1", af.create());
    Region r2 = r1.createSubregion("r2", af.create());
    r2.createSubregion("r3", af.create());

    TransactionListener tl1 = new TransactionListenerAdapter() {
        public void afterCommit(TransactionEvent e) {
          assertEquals(6, e.getEvents().size());
          ArrayList keys = new ArrayList();
          Iterator it = e.getEvents().iterator();
          while (it.hasNext()) {
            EntryEvent ee = (EntryEvent)it.next();
            keys.add(ee.getKey());
            assertEquals(callbackArg, ee.getCallbackArgument());
            assertEquals(true, ee.isCallbackArgumentAvailable());
          }
          assertEquals(CallbackArgDUnitTest.this.expectedKeys, keys);
          CallbackArgDUnitTest.this.invokeCount = 1;
        }
      };
    CacheTransactionManager ctm =  getCache().getCacheTransactionManager();
    ctm.addListener(tl1);

    this.invokeCount = 0;
    this.clCount = 0;
    this.expectedKeys = Arrays.asList(new String[]{"b", "c", "a", "a2", "c2", "b2"});
    doCommitOtherVm();
    assertEquals(1, this.invokeCount);
    assertEquals(6, this.clCount);
  }
}
