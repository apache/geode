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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.fail;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.awaitility.GeodeAwaitility;


public class ScheduledExecutorJUnitTest {

  protected Cache cache = null;
  protected Region regionpr = null;


  protected void createCachePR() {
    Properties props = new Properties();
    props.put(MCAST_PORT, "0");
    props.put(LOCATORS, "");
    cache = new CacheFactory(props).create();

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1);
    paf.setRecoveryDelay(-1);
    paf.setStartupRecoveryDelay(900000);
    PartitionAttributes prAttr = paf.create();

    regionpr = cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(prAttr)
        .create("testRegionPR");
  }

  @Test
  public void testShutdownThreadPool() throws Exception {
    createCachePR();
    regionpr.put("key", "value");
    regionpr.put("key1", "value1");

    Runnable waitForCacheClose = new Runnable() {
      @Override
      public void run() {
        try {
          cache.close();
        } catch (Exception e) {
          fail("Exception");
        }
      }
    };
    waitForCacheClose.run();
    GeodeAwaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> cache.isClosed());

  }

}
