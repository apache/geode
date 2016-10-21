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

import static org.junit.Assert.*;

import java.io.File;
import java.util.Arrays;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * Confirms the bug 37241 is fixed. CleanupFailedInitialization on should also clean disk files
 * created
 */
@Category(DistributedTest.class)
public class Bug37241DUnitTest extends JUnit4DistributedTestCase {

  private static Cache cache = null;

  static VM server1 = null;

  static VM server2 = null;

  private static final String REGION_NAME = "Bug37241DUnitTest_region";

  static final String expectedReplyException = ReplyException.class.getName();

  static final String expectedException = IllegalStateException.class.getName();

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
  }

  /*
   * 1.Create persistent region serevr1 with scope global. 2.Try to create persitent region with
   * same name on server2 with scope d-ack. 3.Region creation should fail . Check for all files
   * created in the directory for server 2 gets deleted.
   */
  @Test
  public void testBug37241ForNewDiskRegion() {
    server1.invoke(() -> Bug37241DUnitTest.createRegionOnServer1());

    try {
      server2.invoke(() -> Bug37241DUnitTest.createRegionOnServer2(Scope.DISTRIBUTED_ACK));
    } catch (Exception e) {
      server2.invoke(() -> Bug37241DUnitTest.ignoreExceptionInLogs());
      server2.invoke(() -> Bug37241DUnitTest.checkForCleanup());
    }
  }

  @Test
  public void testBug37241ForRecreatedDiskRegion() {
    server1.invoke(() -> Bug37241DUnitTest.createRegionOnServer1());
    server2.invoke(() -> Bug37241DUnitTest.createRegionOnServer2(Scope.GLOBAL));
    server2.invoke(() -> Bug37241DUnitTest.closeRegion());
    try {
      server2.invoke(() -> Bug37241DUnitTest.createRegionOnServer2(Scope.DISTRIBUTED_ACK));
    } catch (Exception e) {
      server2.invoke(() -> Bug37241DUnitTest.ignoreExceptionInLogs());
      server2.invoke(() -> Bug37241DUnitTest.checkForCleanupAfterRecreation());
    }
  }

  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    assertNotNull(ds);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static void createRegionOnServer1() throws Exception {
    new Bug37241DUnitTest().createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.GLOBAL);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);

    File[] dirs = new File[2];
    File file1 = new File("server1_disk1");
    File file2 = new File("server1_disk2");
    file1.mkdir();
    file2.mkdir();
    dirs[0] = file1;
    dirs[1] = file2;
    factory.setDiskSynchronous(false);
    factory.setDiskStoreName(
        cache.createDiskStoreFactory().setDiskDirs(dirs).create("Bug37241DUnitTest").getName());
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
  }

  public static void createRegionOnServer2(Scope scope) throws Exception {
    new Bug37241DUnitTest().createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(scope);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);

    File[] dirs = new File[2];
    File file1 = new File("server2_disk1");
    File file2 = new File("server2_disk2");
    file1.mkdir();
    file2.mkdir();
    dirs[0] = file1;
    dirs[1] = file2;
    factory.setDiskSynchronous(false);
    factory.setDiskStoreName(
        cache.createDiskStoreFactory().setDiskDirs(dirs).create("Bug37241DUnitTest").getName());

    // added for not to log exepected IllegalStateExcepion.
    LogWriterUtils.getLogWriter()
        .info("<ExpectedException action=add>" + expectedReplyException + "</ExpectedException>");
    LogWriterUtils.getLogWriter()
        .info("<ExpectedException action=add>" + expectedException + "</ExpectedException>");
    cache.getLogger()
        .info("<ExpectedException action=add>" + expectedReplyException + "</ExpectedException>");
    cache.getLogger()
        .info("<ExpectedException action=add>" + expectedException + "</ExpectedException>");

    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);

  }

  public static void checkForCleanup() {
    try {
      Thread.sleep(200);
    } catch (InterruptedException ignore) {
    }
    cache.getLogger()
        .info("checkForCleanup=" + Arrays.asList(new File("server2_disk2").listFiles()));
    assertEquals(0, new File("server2_disk2").listFiles().length);
  }


  public static void checkForCleanupAfterRecreation() {
    checkForCleanup();
  }

  public static void ignoreExceptionInLogs() {

    cache.getLogger()
        .info("<ExpectedException action=remove>" + expectedException + "</ExpectedException>");

    cache.getLogger().info(
        "<ExpectedException action=remove>" + expectedReplyException + "</ExpectedException>");
    LogWriterUtils.getLogWriter()
        .info("<ExpectedException action=remove>" + expectedException + "</ExpectedException>");
    LogWriterUtils.getLogWriter().info(
        "<ExpectedException action=remove>" + expectedReplyException + "</ExpectedException>");
  }

  public static void closeRegion() {
    Cache cache = CacheFactory.getAnyInstance();
    Region region = cache.getRegion("/" + REGION_NAME);
    region.close();
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  @Override
  public final void preTearDown() throws Exception {
    server1.invoke(() -> Bug37241DUnitTest.closeCache());
    server2.invoke(() -> Bug37241DUnitTest.closeCache());
  }
}


