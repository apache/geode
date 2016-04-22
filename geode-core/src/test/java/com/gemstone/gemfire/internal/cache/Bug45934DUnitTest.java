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

import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.internal.util.DelayedAction;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;

public class Bug45934DUnitTest extends CacheTestCase {
  public Bug45934DUnitTest(String name) {
    super(name);
  }
  
  public void testNormal() throws Exception {
    int count = 1000;
    Host host = Host.getHost(0);
    final VM remote = host.getVM(1);
    final String name = getName();

    // 0. create the remote region, set error flag
    createRemoteRegion(remote, name);

    // 1. create the local cache
    CacheFactory cf = new CacheFactory();
    cf.set("mcast-port", "45934");
    cf.set("conserve-sockets", "false");
    Cache cache = getCache(cf);

    // 2. create normal region locally
    RegionFactory<Integer, Integer> rf = cache.<Integer, Integer> createRegionFactory();
    rf.setDataPolicy(DataPolicy.NORMAL);
    rf.setScope(Scope.DISTRIBUTED_ACK);
    Region<Integer, Integer> region = rf.create(name);
    
    // 3. reset the error flag after initial failure
    AbstractUpdateOperation.test_InvalidVersionAction = new DelayedAction(new Runnable() {
      @Override
      public void run() {
        unsetRemoteFlag(remote);
      }
    });
    AbstractUpdateOperation.test_InvalidVersionAction.allowToProceed();

    // 3. put data
    Map<Integer, Integer> values = new HashMap<Integer, Integer>();
    for (int i = 0; i < count; i++) {
      values.put(i, i);
    }
    region.putAll(values);

    // 5. double check
    verifyLocal(region, count);
    verify(remote, name, count);
    
    cache.close();
  }
  
  private void createRemoteRegion(final VM remote, final String name) {
    SerializableCallable create = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        CacheFactory cf = new CacheFactory();
        cf.set("mcast-port", "45934");
        cf.set("conserve-sockets", "false");

        getCache(cf).<Integer, Integer> createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT)
            .create(name);
        
        AbstractUpdateOperation.test_InvalidVersion = true;
        return null;
      }
    };
    remote.invoke(create);
  }

  private void unsetRemoteFlag(final VM remote) {
    SerializableCallable create = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        AbstractUpdateOperation.test_InvalidVersion = false;
        return null;
      }
    };
    remote.invoke(create);
  }

  private void verifyLocal(Region<Integer, Integer> r, int count) {
    assertEquals(count, r.size());
    for (int i = 0; i < count; i++) {
      assertEquals(i, (int) r.get(i));
    }
  }
  
  private void verify(VM vm, final String name, final int count) {
    SerializableCallable verify = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region<Integer, Integer> r = getCache().getRegion(name);
        assertEquals(count, r.size());
        for (int i = 0; i < count; i++) {
          assertEquals(i, (int) r.get(i));
        }
        return null;
      }
    };
    vm.invoke(verify);
  }
}
