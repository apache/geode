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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.dunit.DistributedTestCase;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;

/**
 * TRAC #33359: AssertionError thrown while processing
 * cache.DistributedClearOperation$ClearRegionMessage
 */

public class ClearRegionMessageAssertionRegressionTest extends DistributedTestCase {

  private static Cache cache;
  private static DistributedSystem ds = null;
  private static Region region;

  @Before
  public void setUp() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    vm0.invoke(this::createCacheVM0);
    vm1.invoke(this::createCacheVM1);
  }

  @After
  public void tearDown() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    vm0.invoke(this::closeCache);
    vm1.invoke(this::closeCache);
  }

  private void createCacheVM0() {
    ds = getSystem();
    cache = CacheFactory.create(ds);

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setEarlyAck(true);
    DistributedSystem.setThreadsSocketPolicy(false);
    RegionAttributes attr = factory.create();

    region = cache.createRegion("map", attr);
  }

  private void createCacheVM1() {
    ds = getSystem();
    DistributedSystem.setThreadsSocketPolicy(false);

    cache = CacheFactory.create(ds);

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);

    RegionAttributes attr = factory.create();

    region = cache.createRegion("map", attr);
  }

  private void closeCache() {
    cache.close();
    ds.disconnect();
  }

  @Test
  public void testClearMultiVM() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    vm0.invoke(new CacheSerializableRunnable("put initial data") {
      @Override
      public void run2() throws CacheException {
        for (int i = 0; i < 10; i++) {
          region.put(i, Integer.toString(i));
        }
      }
    });

    vm0.invoke(new CacheSerializableRunnable("perform clear on region") {
      @Override
      public void run2() throws CacheException {
        region.clear();
      }
    });
  }
}
