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
package org.apache.geode.cache30;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.logging.LoggingThread;

/**
 * Tests populating a region with data that is ever-increasing in size. It is used for testing the
 * "Heap LRU" feature that helps prevent out of memory errors.
 */
public class TestHeapLRU {

  public static void main(String[] args) throws Exception {
    DistributedSystem system = DistributedSystem.connect(new java.util.Properties());
    Cache cache = CacheFactory.create(system);
    AttributesFactory factory = new AttributesFactory();

    factory.setEvictionAttributes(
        EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK));
    factory.setDiskSynchronous(true);
    factory.setDiskStoreName(cache.createDiskStoreFactory()
        .setDiskDirs(new java.io.File[] {new java.io.File(System.getProperty("user.dir"))})
        .create("TestHeapLRU").getName());
    Region region = cache.createRegion("TestDiskRegion", factory.create());

    Thread thread = new LoggingThread("Annoying thread", () -> {
      try {
        while (true) {
          System.out.println("Annoy...");
          Object[] array = new Object[10 /* * 1024 */];
          for (int i = 0; i < array.length; i++) {
            array[i] = new byte[1024];
            Thread.sleep(10);
          }

          System.out.println("SYSTEM GC");
          System.gc();
          Thread.sleep(1000);
        }

      } catch (InterruptedException ex) {
        System.err.println("Interrupted"); // FIXME should throw
      }
    });
    // thread.start();

    // ArrayList list = new ArrayList();
    for (int i = 0; i < Integer.MAX_VALUE; i++) {
      if (i % 1000 == 0) {
        // System.out.println("i = " + i);
        // list = new ArrayList();

      } else {
        // list.add(new Integer(i));
      }

      Integer key = new Integer(i % 10000);
      long[] value = new long[2000];
      // System.out.println("Put " + key + " -> " + value);
      region.put(key, value);
    }
  }

}
