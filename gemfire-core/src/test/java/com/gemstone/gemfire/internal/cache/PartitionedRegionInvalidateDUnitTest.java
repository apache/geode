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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache.util.CacheWriterAdapter;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * This tests invalidateRegion functionality on partitioned regions
 * @author sbawaska
 *
 */
public class PartitionedRegionInvalidateDUnitTest extends
    CacheTestCase {

  public PartitionedRegionInvalidateDUnitTest(String name) {
    super(name);
  }

  void createRegion(String name, boolean accessor, int redundantCopies) {
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(new PartitionAttributesFactory()
        .setLocalMaxMemory(accessor ? 0 : 12)
        .setRedundantCopies(redundantCopies).create());
    getCache().createRegion(name, af.create());
  }
    
  public void testSingleVMInvalidate() {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    final String rName = getUniqueName();
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        createRegion(rName, false, 0);
        Region r = getCache().getRegion(rName);
        InvalidatePRListener l = new InvalidatePRListener();
        r.getAttributesMutator().addCacheListener(l);
        for (int i=0; i<=113; i++) {
          r.put(i, "value"+i);
        }
        PartitionedRegion pr = (PartitionedRegion)r;
        assertTrue(pr.getDataStore().getAllLocalBuckets().size()==113);
        for (Object v : pr.values()) {
          assertNotNull(v);
        }
        r.invalidateRegion();
        assertTrue(l.afterRegionInvalidateCalled);
        l.afterRegionInvalidateCalled = false;
        for (int i=0; i<=113; i++) {
          r.put(i, "value"+i);
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
  
  public void testMultiVMInvalidate() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final String rName = getUniqueName();

    class CreateRegion extends SerializableCallable {
      boolean originRemote;
      public CreateRegion(boolean originRemote) {
        this.originRemote = originRemote;
      }
      public Object call() throws Exception {
        createRegion(rName, false, 1);
        Region r = getCache().getRegion(rName);
        InvalidatePRListener l = new InvalidatePRListener();
        l.originRemote = originRemote;
        r.getAttributesMutator().addCacheListener(l);
        InvalidatePRWriter w = new InvalidatePRWriter();
        r.getAttributesMutator().setCacheWriter(w);
        return null;
      }
    };
    vm0.invoke(new CreateRegion(true));
    vm1.invoke(new CreateRegion(false));
    vm2.invoke(new CreateRegion(true));
    vm3.invoke(new CreateRegion(true));

    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getCache().getRegion(rName);
        for (int i=0; i<=113; i++) {
          r.put(i, "value"+i);
        }
        for (int i=0; i<=113; i++) {
          assertNotNull(r.get(i));
        }
        r.invalidateRegion();

        return null;
      }
    });
    SerializableCallable validateCallbacks  = new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getCache().getRegion(rName);
        InvalidatePRListener l = (InvalidatePRListener)r.getAttributes().getCacheListeners()[0];
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
      public Object call() throws Exception {
        Region r = getCache().getRegion(rName);
        InvalidatePRListener l = (InvalidatePRListener)r.getAttributes().getCacheListeners()[0];
        for (int i=0; i<=113; i++) {
          r.put(i, "value"+i);
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
      public Object call() throws Exception {
        Region r = getCache().getRegion(rName);
        for (int i=0; i<=113; i++) {
          assertNull("Expected null but was "+r.get(i), r.get(i));
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
    public void beforeRegionDestroy(RegionEvent event)
        throws CacheWriterException {
      fail("writer should not have been called");
    }
    @Override
    public void beforeRegionClear(RegionEvent event)
        throws CacheWriterException {
      fail("writer should not have been called");
    }
  
  }
}
