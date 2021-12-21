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
 * is distributed o * or implied. See the License for the specific language governing permissions
 * and limitations under n an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express the License.
 */
package org.apache.geode.cache;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedSystem;

public class RemoveAllCacheListenerPeerRegressionTest {
  private Cache cache;
  private DistributedSystem ds;
  String errStr = null;
  private final String NON_EXISTENT_KEY = "nonExistentKey";


  @Before
  public void setUp() throws Exception {
    Properties p = new Properties();
    p.setProperty(MCAST_PORT, "0");
    p.setProperty(LOCATORS, "");
    ds = DistributedSystem.connect(p);
    cache = CacheFactory.create(ds);
    errStr = null;
  }

  @After
  public void tearDown() throws Exception {
    if (cache != null) {
      cache.close();
      cache = null;
    }
    if (ds != null) {
      ds.disconnect();
      ds = null;
    }
  }

  @Test
  public void removeAllListenerPeerReplicateTest() throws Exception {
    doRemoveAllTest(RegionShortcut.REPLICATE);
  }

  @Test
  public void removeAllListenerPeerPartitionTest() throws Exception {
    doRemoveAllTest(RegionShortcut.PARTITION);
  }

  @Test
  public void destroyListenerPeerReplicateTest() throws Exception {
    doDestroyTest(RegionShortcut.REPLICATE);
  }

  @Test
  public void destroyListenerPeerPartitionTest() throws Exception {
    doDestroyTest(RegionShortcut.PARTITION);
  }

  @Test
  public void removeListenerPeerReplicateTest() throws Exception {
    doRemoveTest(RegionShortcut.REPLICATE);
  }

  @Test
  public void removeListenerPeerPartitionTest() throws Exception {
    doRemoveTest(RegionShortcut.PARTITION);
  }

  private class TestListener extends CacheListenerAdapter {

    @Override
    public void afterDestroy(EntryEvent event) {
      if (event.getKey().equals(NON_EXISTENT_KEY)) {
        errStr = "event fired for non-existent key " + event.getKey() + "; " + event + "\n"
            + getStackTrace();
      }
    }

  }

  private void doRemoveAllTest(RegionShortcut shortcut) {
    RegionFactory<Object, Object> factory = cache.createRegionFactory(shortcut);
    factory.addCacheListener(new TestListener());
    Region aRegion = factory.create("TestRegion");
    aRegion.put("key1", "value1");
    aRegion.put("key2", "value2");
    aRegion.put("key3", "value3");

    List<String> removeAllColl = new ArrayList<>();
    removeAllColl.add("key1");
    removeAllColl.add("key2");
    removeAllColl.add(NON_EXISTENT_KEY);
    aRegion.removeAll(removeAllColl);
    assertNull(errStr); // errStr is set if we invoke afterDestroy in the listener
  }

  private void doDestroyTest(RegionShortcut shortcut) {
    RegionFactory<Object, Object> factory = cache.createRegionFactory(shortcut);
    factory.addCacheListener(new TestListener());
    Region aRegion = factory.create("TestRegion");
    try {
      aRegion.destroy(NON_EXISTENT_KEY);
      fail(EntryNotFoundException.class.getName()
          + " was not thrown when destroying a non-existent key");
    } catch (EntryNotFoundException e) {
      // expected
    } finally {
      assertNull(errStr); // errStr is set if we invoke afterDestroy in the listener
    }
  }

  private void doRemoveTest(RegionShortcut shortcut) {
    RegionFactory<Object, Object> factory = cache.createRegionFactory(shortcut);
    factory.addCacheListener(new TestListener());
    Region aRegion = factory.create("TestRegion");
    Object returnedValue = aRegion.remove(NON_EXISTENT_KEY);
    assertNull(returnedValue);
    assertNull(errStr); // errStr is set if we invoke afterDestroy in the listener
  }

  /** Get a stack trace of the current stack and return it as a String */
  public static String getStackTrace() {
    try {
      throw new Exception("Exception to get stack trace");
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw, true));
      return sw.toString();
    }
  }

}
