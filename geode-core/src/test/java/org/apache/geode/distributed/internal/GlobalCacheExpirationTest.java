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

package org.apache.geode.distributed.internal;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.internal.cache.ColocationLogger;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.MembershipTest;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import java.util.Random;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;

@Category({DistributedTest.class, MembershipTest.class})
public class GlobalCacheExpirationTest extends JUnit4CacheTestCase {
  private static final Logger logger = (Logger) LogManager.getLogger();
  private static ArgumentCaptor<LogEvent> loggingEventCaptor;
  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Override
  public void preSetUp() throws Exception {
    disconnectAllFromDS();
  }

  static Region otherRegion;

  @Test
  public void TokensDereferencedOnExpirationFromGlobalCache() throws InterruptedException {
    Cache myCache = getCache();
    DistributedRegion region = (DistributedRegion) myCache.createRegionFactory()
        .setScope(Scope.GLOBAL).setDataPolicy(DataPolicy.REPLICATE)
        .setEntryTimeToLive(new ExpirationAttributes(1, ExpirationAction.DESTROY))
        .create("testRegion");

    region.becomeLockGrantor();

    Host host = Host.getHost(0);
    VM vm1 = host.getVM(1);

    vm1.invoke(new SerializableRunnable("Connect to distributed system") {
      public void run() {
        Cache myCache = getCache();
        otherRegion =
            myCache.createRegionFactory().setScope(Scope.GLOBAL).setDataPolicy(DataPolicy.REPLICATE)
                .setEntryTimeToLive(new ExpirationAttributes(1, ExpirationAction.DESTROY))
                .create("testRegion");
      }
    });

    Random rand = new Random();
    for (int i = 0; i < 10000; i++) {
      String key = Integer.toString(i);
      region.put(key, Integer.toString(rand.nextInt()));
    }

    Assert.assertTrue(region.size() > 0);
    vm1.invoke(new SerializableRunnable("disconnect from ds") {
      public void run() {
        Assert.assertTrue(otherRegion.size() > 0);
      }
    });
    Thread.sleep(2000);
    vm1.invoke(new SerializableRunnable("check size") {
      public void run() {
        Assert.assertEquals(0, otherRegion.size());
      }
    });
    Assert.assertTrue(region.isLockGrantor());
    Assert.assertEquals(0, region.size());


    DLockService service = (DLockService) region.getLockService();
    Assert.assertTrue(service.isLockGrantor());
    Assert.assertEquals(0, service.getStats().getTokens());
  }

  public static Region region1;
  public static Region region2;

  @Test
  public void DataConsistencyTest() throws InterruptedException {
    DistributionManager dm = (DistributionManager) getSystem().getDistributionManager();
    /*
     * Cache myCache = getCache(); RegionFactory<String, String> factory =
     * myCache.createRegionFactory(); Region<String, String> region = factory
     * .setDataPolicy(DataPolicy.PARTITION) .setPartitionAttributes(new
     * PartitionAttributesFactory<>().setRedundantCopies(1).create())
     * .create("testPartitionRegion");
     */

    Host host = Host.getHost(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    vm1.invoke(new SerializableRunnable("Connect to distributed system") {
      public void run() {
        Cache myCache = getCache();
        RegionFactory<String, String> factory = myCache.createRegionFactory();
        region1 = factory.setDataPolicy(DataPolicy.PARTITION)
            .setPartitionAttributes(
                new PartitionAttributesFactory<>().setRedundantCopies(1).create())
            .create("testPartitionRegion");
      }
    });

    vm2.invoke(new SerializableRunnable("Connect to distributed system") {
      public void run() {
        Cache myCache = getCache();
        RegionFactory<String, String> factory = myCache.createRegionFactory();
        region2 = factory.setDataPolicy(DataPolicy.PARTITION)
            .setPartitionAttributes(
                new PartitionAttributesFactory<>().setRedundantCopies(1).create())
            .create("testPartitionRegion");
      }
    });

    vm1.invoke(new SerializableRunnable("load data") {
      public void run() {
        Random rand = new Random();
        for (int i = 0; i < 1000; i++) {
          String key = Integer.toString(i);
          region1.put(key, Integer.toString(rand.nextInt()));
        }
      }
    });

    /*
     * for (int i = 0; i < 10; i++) { Assert.assertNotNull(regionAccessor.get(Integer.toString(i)));
     * }
     */

    RegionFactory<String, String> factory = getCache().createRegionFactory();
    Region regionAccessor = factory.setDataPolicy(DataPolicy.PARTITION)
        .setPartitionAttributes(
            new PartitionAttributesFactory<>().setRedundantCopies(1).setLocalMaxMemory(0).create())
        .create("testPartitionRegion");

    vm2.invoke(new SerializableRunnable("check size") {
      public void run() {
        Assert.assertEquals(1000, region2.size());
      }
    });

    vm1.invoke(new SerializableRunnable("check size") {
      public void run() {
        Assert.assertEquals(1000, region1.size());
      }
    });

    // regionAccessor.get("postDisconnect1Key");

    vm1.invoke(new SerializableRunnable("disconnect") {
      public void run() {
        region1.close();
      }
    });

    vm2.invoke(new SerializableRunnable("load data") {
      public void run() {
        region2.put("postDisconnect1Key", "pastDisconnect1Value");
      }
    });

    // Object result = regionAccessor.get("postDisconnect1Key");
    // Assert.assertNotNull(result);
    Appender mockAppender = attatchMockAppenderToLogger();
    vm2.invoke(new SerializableRunnable("disconnect") {
      public void run() {
        // getSystem().emergencyClose();
        // getSystem().disconnect();
        region2.close();
      }
    });
    ArgumentCaptor<LogEvent> loggingEventCaptor = ArgumentCaptor.forClass(LogEvent.class);
    detatchMockAppenderFromLogger(mockAppender);
    // Thread.sleep(4000);
    // Object resultPostBounce = regionAccessor.get("postDisconnect1Key");
    // Assert.assertNotNull(resultPostBounce);


    /*
     * try { Object postDisconnectResult = regionAccessor.get("1"); } catch
     * (PartitionedRegionStorageException e) { logger.log(Level.ERROR, e); }
     * System.out.println("Done");
     */
  }

  private Appender attatchMockAppenderToLogger() {
    Appender mockAppender = mock(Appender.class);
    when(mockAppender.getName()).thenReturn("MockAppender");
    when(mockAppender.isStarted()).thenReturn(true);
    when(mockAppender.isStopped()).thenReturn(false);
    logger.addAppender(mockAppender);
    logger.setLevel(Level.WARN);
    loggingEventCaptor = ArgumentCaptor.forClass(LogEvent.class);
    return mockAppender;
  }

  private void detatchMockAppenderFromLogger(Appender appender) {
    logger.removeAppender(appender);
  }
}
