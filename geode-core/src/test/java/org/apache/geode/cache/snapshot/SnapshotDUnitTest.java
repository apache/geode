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
package org.apache.geode.cache.snapshot;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import java.io.File;
import java.io.FilenameFilter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import com.examples.snapshot.MyObject;
import com.examples.snapshot.MyPdxSerializer;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.snapshot.RegionGenerator.RegionType;
import org.apache.geode.cache.snapshot.RegionGenerator.SerializationType;
import org.apache.geode.cache.snapshot.SnapshotOptions.SnapshotFormat;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.cache30.CacheTestCase;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;

@Category(DistributedTest.class)
public class SnapshotDUnitTest extends JUnit4CacheTestCase {
  public SnapshotDUnitTest() {
    super();
  }

  @Test
  public void testExportAndImport() throws Exception {
    File dir = new File(getDiskDirs()[0], "snap");
    dir.mkdir();
    
    // save all regions
    getCache().getSnapshotService().save(dir, SnapshotFormat.GEMFIRE);

    for (final RegionType rt : RegionType.values()) {
      for (final SerializationType st : SerializationType.values()) {
        String name = "test-" + rt.name() + "-" + st.name();

        // overwrite region with bad data
        Region<Integer, MyObject> region = getCache().getRegion(name);
        for (Entry<Integer, MyObject> entry : region.entrySet()) {
          region.put(entry.getKey(), new MyObject(Integer.MAX_VALUE, "bad!!"));
        }
      }
    }
        
    SerializableCallable callbacks = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        for (final RegionType rt : RegionType.values()) {
          for (final SerializationType st : SerializationType.values()) {
            String name = "test-" + rt.name() + "-" + st.name();

            Cache c = getCache();
            Region<Integer, MyObject> region = c.getRegion(name);
            region.getAttributesMutator().setCacheWriter(new CacheWriterAdapter<Integer, MyObject>() {
              @Override
              public void beforeUpdate(EntryEvent<Integer, MyObject> event) {
                fail("CacheWriter invoked during import");
              }
            });
            
            region.getAttributesMutator().addCacheListener(new CacheListenerAdapter<Integer, MyObject>() {
              @Override
              public void afterUpdate(EntryEvent<Integer, MyObject> event) {
                fail("CacheListener invoked during import");
              }
            });
          }
        }
        return null;
      }
    };

    // add callbacks
    forEachVm(callbacks, true);
    
    // load all regions
    RegionGenerator rgen = new RegionGenerator();
    getCache().getSnapshotService().load(dir, SnapshotFormat.GEMFIRE);
    for (final RegionType rt : RegionType.values()) {
      for (final SerializationType st : SerializationType.values()) {
        Region<Integer, MyObject> region = getCache().getRegion("test-" + rt.name() + "-" + st.name());
        for (Entry<Integer, MyObject> entry : createExpected(st, rgen).entrySet()) {
          assertEquals("Comparison failure for " + rt.name() + "/" + st.name(), 
              entry.getValue(), region.get(entry.getKey()));
        }
      }
    }
  }

  @Test
  public void testCacheExportFilterException() throws Exception {
    SnapshotFilter<Object, Object> oops = new SnapshotFilter<Object, Object>() {
      @Override
      public boolean accept(Entry<Object, Object> entry) {
        throw new RuntimeException();
      }
    };
    
    CacheSnapshotService css = getCache().getSnapshotService();
    SnapshotOptions<Object, Object> options = css.createOptions().setFilter(oops);
    
    boolean caughtException = false;
    try {
      File dir = new File(getDiskDirs()[0], "export");
      dir.mkdir();
      
      css.save(dir, SnapshotFormat.GEMFIRE, options);
    } catch (Exception e) {
      caughtException = true;
    }
    
    assertTrue(caughtException);
  }
  
  @Test
  public void testCacheImportFilterException() throws Exception {
    SnapshotFilter<Object, Object> oops = new SnapshotFilter<Object, Object>() {
      @Override
      public boolean accept(Entry<Object, Object> entry) {
        throw new RuntimeException();
      }
    };
    
    File dir = new File(getDiskDirs()[0], "import");
    dir.mkdir();
    
    // save all regions
    CacheSnapshotService css = getCache().getSnapshotService();
    css.save(dir, SnapshotFormat.GEMFIRE);
    
    SnapshotOptions<Object, Object> options = css.createOptions().setFilter(oops);
    
    boolean caughtException = false;
    try {
      css.load(dir.listFiles(), SnapshotFormat.GEMFIRE, options);
    } catch (Exception e) {
      caughtException = true;
    }
    
    assertTrue(caughtException);
  }

  @Override
  public final void postSetUp() throws Exception {
    loadCache();
    
    RegionGenerator rgen = new RegionGenerator();
    for (final RegionType rt : RegionType.values()) {
      for (final SerializationType st : SerializationType.values()) {
        Region<Integer, MyObject> region = getCache().getRegion("test-" + rt.name() + "-" + st.name());
        region.putAll(createExpected(st, rgen));
      }
    }
  }
  
  public static Map<Integer, MyObject> createExpected(SerializationType type, RegionGenerator rgen) {
    Map<Integer, MyObject> expected = new HashMap<Integer, MyObject>();
    for (int i = 0; i < 1000; i++) {
      expected.put(i, rgen.createData(type, i, "The number is " + i));
    }
    return expected;
  }

  public void loadCache() throws Exception {
    SerializableCallable setup = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        CacheFactory cf = new CacheFactory()
          .setPdxSerializer(new MyPdxSerializer())
          .setPdxPersistent(true);
    
        Cache cache = getCache(cf);
        DiskStore ds = cache.createDiskStoreFactory()
            .setMaxOplogSize(1)
            .setDiskDirs(getDiskDirs())
            .create("snapshotTest");
        
        RegionGenerator rgen = new RegionGenerator();
        
        for (final RegionType rt : RegionType.values()) {
          for (final SerializationType st : SerializationType.values()) {
            rgen.createRegion(cache, ds.getName(), rt, "test-" + rt.name() + "-" + st.name());
          }
        }
        return null;
      }
    };
    
    forEachVm(setup, true);
  }
  
  public static Object forEachVm(SerializableCallable call, boolean local) throws Exception {
    Host host = Host.getHost(0);
    int vms = host.getVMCount();
    
    for(int i = 0; i < vms; ++i) {
      host.getVM(i).invoke(call);
    }
    
    if (local) {
      return call.call();
    }
    return null;
  }
}
