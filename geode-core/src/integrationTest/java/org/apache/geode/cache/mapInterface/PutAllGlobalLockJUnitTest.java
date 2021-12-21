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
package org.apache.geode.cache.mapInterface;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertTrue;

import java.util.Properties;
import java.util.TreeMap;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.junit.categories.DLockTest;

@Category({DLockTest.class})
public class PutAllGlobalLockJUnitTest { // TODO: reformat

  Region testRegion = null;
  volatile boolean done = false;
  boolean testOK = false;
  Thread thread;

  @Before
  public void setUp() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");
    DistributedSystem distributedSystem = DistributedSystem.connect(properties);
    Cache cache = CacheFactory.create(distributedSystem);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.GLOBAL);
    factory.setCacheListener(new Listener());
    RegionAttributes regionAttributes = factory.create();
    testRegion = cache.createRegion("TestRegion", regionAttributes);
  }


  @Test
  public void testPutAllGlobalLock() throws Exception {
    TreeMap trialMap = new TreeMap();
    for (long i = 0; i < 1000; i++) {
      trialMap.put(new Long(i), new Long(i));
    }
    testRegion.putAll(trialMap);
    ThreadUtils.join(thread, 30 * 1000);
    assertTrue(testOK);
  }

  protected class Listener extends CacheListenerAdapter {

    @Override
    public void afterCreate(EntryEvent event) {
      if (event.getKey().equals(new Long(1))) {
        thread = new Thread(new Runner());
        thread.start();
      } else if (event.getKey().equals(new Long(999))) {
        done = true;

      }
    }
  }

  protected class Runner implements Runnable {

    @Override
    public void run() {
      testRegion.put(new Long(1000), new Long(1000));
      testOK = done;
    }
  }
}
