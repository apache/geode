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
package com.gemstone.gemfire.cache.snapshot;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;

import com.examples.snapshot.MyObject;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.snapshot.RegionGenerator.SerializationType;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.FilenameFilter;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.MCAST_PORT;

public class SnapshotTestCase {
  protected File store;
  protected File snaps;
  protected Cache cache;
  protected RegionGenerator rgen;
  protected DiskStore ds;

  @Before
  public void setUp() throws Exception {
    store = new File("store-" + Math.abs(new Random().nextInt()));
    store.mkdir();
    
    snaps = new File("snapshots-" + Math.abs(new Random().nextInt()));
    snaps.mkdir();

    rgen = new RegionGenerator();

    CacheFactory cf = new CacheFactory()
        .set(MCAST_PORT, "0")
        .set(LOG_LEVEL, "error");
    cache = cf.create();
    
    ds = cache.createDiskStoreFactory()
        .setMaxOplogSize(1)
        .setDiskDirs(new File[] { store })
        .create("snapshotTest");
  }
  
  @After
  public void tearDown() throws Exception {
    cache.close();
    deleteFiles(store);
    deleteFiles(snaps);
  }
  
  public Map<Integer, MyObject> createExpected(SerializationType type) {
    Map<Integer, MyObject> expected = new HashMap<Integer, MyObject>();
    for (int i = 0; i < 1000; i++) {
      expected.put(i, rgen.createData(type, i, "The number is " + i));
    }
    return expected;
  }

  public static void deleteFiles(File dir) {
    File[] deletes = dir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return true;
      }
    });
    
    if (deletes != null) {
      for (File f : deletes) {
        f.delete();
      }
    }
    dir.delete();
  }
}
