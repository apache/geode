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

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test of interrupting threads doing disk writes to see the effect.
 *
 */
@Category(IntegrationTest.class)
public class InterruptDiskJUnitTest  {

  private static volatile Thread puttingThread;
  private static final long MAX_WAIT = 60 * 1000;
  private DistributedSystem ds;
  private Cache cache;
  private Region<Object, Object> region;
  private ExecutorService ex;
  private AtomicLong nextValue = new AtomicLong();

  @Test
  @Ignore
  public void testLoop() throws Throwable {
    for(int i=0; i < 100; i++) {
      System.err.println("i=" +i);
      System.out.println("i=" +i);
      testDRPutWithInterrupt();
      tearDown();
      setUp();
    }
  }


  @Before
  public void setUp() {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(LOG_LEVEL, "config"); // to keep diskPerf logs smaller
    props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
    props.setProperty(ENABLE_TIME_STATISTICS, "true");
    props.setProperty(STATISTIC_ARCHIVE_FILE, "stats.gfs");
    ds = DistributedSystem.connect(props);
    cache = CacheFactory.create(ds);
    File diskStore = new File("diskStore");
    diskStore.mkdir();
    cache.createDiskStoreFactory().setMaxOplogSize(1).setDiskDirs(new File[] {diskStore} ).create("store");
    region = cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT).setDiskStoreName("store").create("region");
    ex = Executors.newSingleThreadExecutor();
  }


  @After
  public void tearDown() {
    ds.disconnect();
    ex.shutdownNow();
  }


  @Test
  public void testDRPutWithInterrupt() throws Throwable {
    Callable doPuts = new Callable() {

      @Override
      public Object call() {
        puttingThread = Thread.currentThread();
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(MAX_WAIT);
        while(!Thread.currentThread().isInterrupted()) {
          region.put(0, nextValue.incrementAndGet());
          if(System.nanoTime() > end) {
            fail("Did not get interrupted in 60 seconds");
          }
        }
        return null;
      }
    };
    
    Future result = ex.submit(doPuts);
    
    
    Thread.sleep(50);
    long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(MAX_WAIT);
    while(puttingThread == null) {
      Thread.sleep(50);
      if(System.nanoTime() > end) {
        fail("Putting thread not set in 60 seconds");
      }
    }

    puttingThread.interrupt();
    
    result.get(60, TimeUnit.SECONDS);
    
    assertEquals(nextValue.get(), region.get(0));
    
  }
}