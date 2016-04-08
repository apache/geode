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
package com.gemstone.gemfire.internal.cache;

import java.io.File;
import java.io.FilenameFilter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import junit.framework.TestCase;

import com.examples.snapshot.MyObject;
import com.examples.snapshot.MyPdxSerializer;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.snapshot.RegionGenerator;
import com.gemstone.gemfire.cache.snapshot.RegionGenerator.RegionType;
import com.gemstone.gemfire.cache.snapshot.RegionGenerator.SerializationType;
import com.gemstone.gemfire.cache.snapshot.SnapshotIterator;
import com.gemstone.gemfire.cache.snapshot.SnapshotReader;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class OfflineSnapshotJUnitTest {
  private RegionGenerator rgen;
  
  private Cache cache;
  private DiskStore ds;
  
  @Test
  public void testExport() throws Exception {
    int rcount = 0;
    for (final RegionType rt : RegionType.persistentValues()) {
      for (final SerializationType st : SerializationType.offlineValues()) {
        Region<Integer, MyObject> region = rgen.createRegion(cache, ds.getName(), rt, "test" + rcount++);
        final Map<Integer, MyObject> expected = createExpected(st, 1000);
        
        region.putAll(expected);
        cache.close();
        
        DiskStoreImpl.exportOfflineSnapshot(ds.getName(), new File[] { new File(".") }, new File("."));
        SnapshotTestUtil.checkSnapshotEntries(new File("."), expected, ds.getName(), region.getName());
        
        reset();
      }
    }
  }
  
  @Test
  public void testLargeFileExport() throws Exception {
    int count = 10000;
    Region<Integer, MyObject> region = rgen.createRegion(cache, ds.getName(), RegionType.PARTITION_PERSISTENT, "test");

    System.out.println("Creating entries...");
    final Map<Integer, MyObject> expected = createExpected(SerializationType.DATA_SERIALIZABLE, count);
    
    region.putAll(expected);
    cache.close();
    
    System.out.println("Recovering entries...");
    for (int i = 0; i < 10; i++) {
      long start = System.currentTimeMillis();
      DiskStoreImpl.exportOfflineSnapshot(ds.getName(), new File[] { new File(".") }, new File("."));
      
      long elapsed = System.currentTimeMillis() - start;
      double rate = 1.0 * count / elapsed;
      
      System.out.println("Created snapshot with " + count + " entries in " + elapsed + " ms (" + rate + " entries/ms)");
      SnapshotTestUtil.checkSnapshotEntries(new File("."), expected, ds.getName(), region.getName());
    }
  }
  
  public Map<Integer, MyObject> createExpected(SerializationType type, int count) {
    Map<Integer, MyObject> expected = new HashMap<Integer, MyObject>();
    for (int i = 0; i < count; i++) {
      expected.put(i, rgen.createData(type, i, "The number is " + i));
    }
    return expected;
  }
  
  @Before
  public void setUp() throws Exception {
    for (File f : new File(".").listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith("BACKUP") || name.startsWith("snapshot-");
      }
    })) {
      f.delete();
    }

    reset();
    rgen = new RegionGenerator();
  }
  
  @After
  public void tearDown() throws Exception {
    if (!cache.isClosed()) {
      cache.close();
    }
  }

  public void reset() {
    CacheFactory cf = new CacheFactory()
        .set("mcast-port", "0")
        .set("log-level", "error")
        .setPdxSerializer(new MyPdxSerializer())
        .setPdxPersistent(true);
    
    cache = cf.create();
    ds = cache.createDiskStoreFactory().setMaxOplogSize(1).create("snapshotTest");
  }
}
