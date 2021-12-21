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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;

/**
 * This tests invalidateRegion functionality on partitioned regions
 *
 */

public class PartitionedRegionInvalidateDUnitTest extends JUnit4CacheTestCase {

  public PartitionedRegionInvalidateDUnitTest() {
    super();
  }

  void createRegion(String name, boolean accessor, int redundantCopies, CacheWriter w) {
    AttributesFactory af = new AttributesFactory();
    af.setCacheWriter(w);
    af.setPartitionAttributes(new PartitionAttributesFactory().setLocalMaxMemory(accessor ? 0 : 12)
        .setRedundantCopies(redundantCopies).create());
    getCache().createRegion(name, af.create());
  }

  @Test
  public void testSingleVMInvalidate() {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    final String rName = getUniqueName();
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createRegion(rName, false, 0, null);
        Region r = getCache().getRegion(rName);
        InvalidatePRListener l = new InvalidatePRListener();
        r.getAttributesMutator().addCacheListener(l);
        for (int i = 0; i <= 113; i++) {
          r.put(i, "value" + i);
        }
        PartitionedRegion pr = (PartitionedRegion) r;
        assertTrue(pr.getDataStore().getAllLocalBuckets().size() == 113);
        for (Object v : pr.values()) {
          assertNotNull(v);
        }
        r.invalidateRegion();
        assertTrue(l.afterRegionInvalidateCalled);
        l.afterRegionInvalidateCalled = false;
        for (int i = 0; i <= 113; i++) {
          r.put(i, "value" + i);
        }
        Object callbackArg = "CallBACK";
        l.callbackArg = callbackArg;
        r.invalidateRegion(callbackArg);
        assertTrue(l.afterRegionInvalidateCalled);
        l.afterRegionInvalidateCalled = false;
        return null;
      }
    });
  }

  @Test
  public void testMultiVMInvalidate() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final String rName = getUniqueName();

    class CreateRegion extends SerializableCallable {
      final boolean originRemote;

      public CreateRegion(boolean originRemote) {
        this.originRemote = originRemote;
      }

      @Override
      public Object call() throws Exception {
        InvalidatePRWriter w = new InvalidatePRWriter();
        createRegion(rName, false, 1, w);
        Region r = getCache().getRegion(rName);
        InvalidatePRListener l = new InvalidatePRListener();
        l.originRemote = originRemote;
        r.getAttributesMutator().addCacheListener(l);
        return null;
      }
    }
    vm0.invoke(new CreateRegion(true));
    vm1.invoke(new CreateRegion(false));
    vm2.invoke(new CreateRegion(true));
    vm3.invoke(new CreateRegion(true));

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getCache().getRegion(rName);
        for (int i = 0; i <= 113; i++) {
          r.put(i, "value" + i);
        }
        for (int i = 0; i <= 113; i++) {
          assertNotNull(r.get(i));
        }
        r.invalidateRegion();

        return null;
      }
    });
    SerializableCallable validateCallbacks = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getCache().getRegion(rName);
        InvalidatePRListener l = (InvalidatePRListener) r.getAttributes().getCacheListeners()[0];
        assertTrue(l.afterRegionInvalidateCalled);
        l.afterRegionInvalidateCalled = false;

        l.callbackArg = "CallBACK";
        return null;
      }
    };
    vm0.invoke(validateCallbacks);
    vm1.invoke(validateCallbacks);
    vm2.invoke(validateCallbacks);
    vm3.invoke(validateCallbacks);

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getCache().getRegion(rName);
        InvalidatePRListener l = (InvalidatePRListener) r.getAttributes().getCacheListeners()[0];
        for (int i = 0; i <= 113; i++) {
          r.put(i, "value" + i);
        }
        Object callbackArg = "CallBACK";
        l.callbackArg = callbackArg;
        r.invalidateRegion(callbackArg);

        return null;
      }
    });

    vm0.invoke(validateCallbacks);
    vm1.invoke(validateCallbacks);
    vm2.invoke(validateCallbacks);
    vm3.invoke(validateCallbacks);

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getCache().getRegion(rName);
        for (int i = 0; i <= 113; i++) {
          assertNull("Expected null but was " + r.get(i), r.get(i));
        }
        return null;
      }
    });
  }

  class InvalidatePRListener extends CacheListenerAdapter {
    Object callbackArg;
    boolean originRemote;
    boolean afterRegionInvalidateCalled;

    @Override
    public void afterInvalidate(EntryEvent event) {
      fail("After invalidate should not be called for individual entry");
    }

    @Override
    public void afterRegionInvalidate(RegionEvent event) {
      afterRegionInvalidateCalled = true;
      assertTrue(event.getOperation().isRegionInvalidate());
      assertEquals(originRemote, event.isOriginRemote());
      if (callbackArg != null) {
        assertTrue(event.isCallbackArgumentAvailable());
        assertEquals(callbackArg, event.getCallbackArgument());
      }
    }
  }

  class InvalidatePRWriter extends CacheWriterAdapter {
    @Override
    public void beforeRegionDestroy(RegionEvent event) throws CacheWriterException {
      fail("writer should not have been called");
    }

    @Override
    public void beforeRegionClear(RegionEvent event) throws CacheWriterException {
      fail("writer should not have been called");
    }

  }
}
