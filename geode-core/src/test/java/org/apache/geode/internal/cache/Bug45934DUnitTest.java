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
package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.util.DelayedAction;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class Bug45934DUnitTest extends JUnit4CacheTestCase {

  @Test
  public void testNormal() throws Exception {
    int count = 1000;
    Host host = Host.getHost(0);
    final VM remote = host.getVM(1);
    final String name = getName();

    // 0. create the remote region, set error flag
    createRemoteRegion(remote, name);

    // 1. create the local cache
    CacheFactory cf = new CacheFactory();
    cf.set(MCAST_PORT, "45934");
    cf.set(CONSERVE_SOCKETS, "false");
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
        cf.set(MCAST_PORT, "45934");
        cf.set(CONSERVE_SOCKETS, "false");

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
