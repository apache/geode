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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.admin.remote.ShutdownAllRequest;

import java.io.File;
import java.util.Properties;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.LOCATORS;
import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.MCAST_PORT;

/**
 *
 */
public class RunCacheInOldGemfire {

  public static final String diskStoreName1 = "ds1";
  public static final String diskStoreName2 = "ds2";
  public static final String regionName1 = "region1";
  public static final String regionName2 = "region2";
  public static final int maxOplogSize = 1;
  public static final int entrySize = 1024;
  public static final int numOfKeys = maxOplogSize * 1024 * 1024 / entrySize;
  
  protected Cache createCache(String mcastPort) {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, mcastPort);
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, "oldgemfire.log");
    InternalDistributedSystem localsystem = (InternalDistributedSystem)DistributedSystem.connect(config);
    Cache cache = CacheFactory.create(localsystem);
    return cache;
  }

  protected DiskStore createDiskStore(Cache cache, String diskStoreName, String dirName) {
    // maxOplogSize==1m
    File dir = new File(dirName);
    dir.mkdirs();

    DiskStore ds = cache.findDiskStore(diskStoreName);
    if(ds == null) {
      ds = cache.createDiskStoreFactory()
      .setDiskDirs(new File[] {dir}).setMaxOplogSize(maxOplogSize).create(diskStoreName);
    }
    return ds;
  }
  
  protected Region createPersistentRegion(Cache cache, String regionName, String diskStoreName, boolean isPR) {
    RegionFactory factory;
    if (isPR) {
      PartitionAttributesFactory paf = new PartitionAttributesFactory();
      paf.setRedundantCopies(1);
      factory = cache.createRegionFactory().setDiskStoreName(diskStoreName)
      .setDataPolicy(DataPolicy.PERSISTENT_PARTITION)
      .setPartitionAttributes(paf.create());
    } else {
      factory = cache.createRegionFactory().setDiskStoreName(diskStoreName)
      .setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    }
    return factory.create(regionName);
  }

  /**
   * start a cache with data
   * 
   * @param args
   */
  public static void main(String[] args) {
    // The main() is used for run by a script under older gemfire version
    if (args.length != 5) {
      System.out.println("Usage: java -cp geode-dependencies.jar:$JTESTS com.gemstone.gemfire.internal.cache.RunCacheInOldGemfire mcastPort diskdir isPR doOps(true or false) isShutDownAll");
    }
    
    String mcastPort = args[0];
    String diskdir = args[1];
    boolean isPR = Boolean.valueOf(args[2]);
    boolean doOps = Boolean.valueOf(args[3]);
    boolean isShutDownAll = Boolean.valueOf(args[4]);

    RunCacheInOldGemfire test = new RunCacheInOldGemfire();
    Cache cache = test.createCache(mcastPort);
    
    // create 2 diskstores using the same directory, which should be different with another cache instance
    test.createDiskStore(cache, diskStoreName1, diskdir);
    test.createDiskStore(cache, diskStoreName2, diskdir);
    // create 2 regions each uses different diskstore
    Region region1 = test.createPersistentRegion(cache, regionName1, diskStoreName1, isPR);
    Region region2 = test.createPersistentRegion(cache, regionName2, diskStoreName2, isPR);
    
    if (doOps) {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      
      for (Region region: new Region[] {region1, region2}) {
        // create enough entries to roll the oplog
        // do some destroys, invalidates
        region.put("key1", "value1");
        for (int i=0; i<numOfKeys; i++) {
          byte [] value = new byte[entrySize];
          region.put(""+i, value);
        }
        region.put("key2", "value2");
        region.put("key3", "value3");
        region.put("key4", "value4");
        region.put("key5", "value5");// create

        region.destroy("key1");
        region.destroy("key2");
        region.invalidate("key3");
        region.put("key5", "value6");// update
      }
    } // doOps
    
    try {
    if (isShutDownAll) {
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      GemFireCacheImpl gfc = (GemFireCacheImpl)cache;
      ShutdownAllRequest.send(gfc.getDistributedSystem().getDistributionManager(), 0);
    } else {
      // wait until all the operations are done 
      final long tilt = System.currentTimeMillis() + 60000;
      while (true) {
        String value1 = (String)region1.get("key5");
        String value2 = (String)region2.get("key5");
        if (value1 != null && value2 != null && value1.equals("value6") && value2.equals("value6")) {
          break;
        } else {
          long timeLeft = tilt - System.currentTimeMillis();
          if (timeLeft <= 0) {
            break;
          }
          try {
            Thread.sleep(2000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      } // while
    }
    } catch (Throwable t) {
      boolean ignore = false;
      if (t instanceof java.lang.Error) {
        if (t.getMessage().contains("Maximum permit count exceeded")) {
          System.out.println("Known issue caused by jdk1.6, ignored for the test:" + t.getMessage());
          ignore = true;
        }
      }
      if (!ignore) {
        throw new RuntimeException(t);
      }
    }
  }
}
