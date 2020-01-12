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
package org.apache.geode.cache.snapshot;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import com.examples.snapshot.MyObject;
import com.examples.snapshot.MyPdxSerializer;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.AsyncEventQueueFactory;
import org.apache.geode.cache.snapshot.RegionGenerator.RegionType;
import org.apache.geode.cache.snapshot.RegionGenerator.SerializationType;
import org.apache.geode.cache.snapshot.SnapshotOptions.SnapshotFormat;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.SnapshotTest;

@Category({SnapshotTest.class})
public class SnapshotDUnitTest extends JUnit4CacheTestCase {

  private static final int NUM_ENTRIES = 1000;

  public SnapshotDUnitTest() {
    super();
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties properties = super.getDistributedSystemProperties();
    properties.put(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        SerializationType.class.getName() + ";" + MyObject.class.getName() + ";"
            + SnapshotProblem.class.getName());
    return properties;
  }

  @Test
  public void testExportAndImport() throws Exception {
    File dir = new File(getDiskDirs()[0], "snap");
    dir.mkdir();

    // save all regions
    getCache().getSnapshotService().save(dir, SnapshotFormat.GEMFIRE);

    // update regions with data to be overwritten by import
    updateRegions();

    SerializableCallable callbacks = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        for (final RegionType rt : RegionType.values()) {
          for (final SerializationType st : SerializationType.values()) {
            String name = "test-" + rt.name() + "-" + st.name();

            Cache c = getCache();
            Region<Integer, MyObject> region = c.getRegion(name);
            region.getAttributesMutator()
                .setCacheWriter(new CacheWriterAdapter<Integer, MyObject>() {
                  @Override
                  public void beforeUpdate(EntryEvent<Integer, MyObject> event) {
                    fail("CacheWriter invoked during import");
                  }
                });

            region.getAttributesMutator()
                .addCacheListener(new CacheListenerAdapter<Integer, MyObject>() {
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
    loadRegions(dir, null);
  }

  @Test
  public void testExportAndImportWithInvokeCallbacksEnabled() throws Exception {
    File dir = new File(getDiskDirs()[0], "callbacks");
    dir.mkdir();

    // save all regions
    CacheSnapshotService service = getCache().getSnapshotService();
    service.save(dir, SnapshotFormat.GEMFIRE);

    // update regions with data to be overwritten by importdir
    updateRegions();

    SerializableCallable callbacks = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        for (final RegionType rt : RegionType.values()) {
          for (final SerializationType st : SerializationType.values()) {
            String name = "test-" + rt.name() + "-" + st.name();
            Cache cache = getCache();
            Region<Integer, MyObject> region = cache.getRegion(name);
            // add CacheWriter and CacheListener
            AttributesMutator mutator = region.getAttributesMutator();
            mutator.setCacheWriter(new CountingCacheWriter());
            mutator.addCacheListener(new CountingCacheListener());
            // add AsyncEventQueue
            addAsyncEventQueue(region, name);
          }
        }
        return null;
      }
    };

    // add callbacks
    forEachVm(callbacks, true);

    // load all regions with invoke callbacks enabled
    SnapshotOptions options = service.createOptions();
    options.invokeCallbacks(true);
    loadRegions(dir, options);

    // verify callbacks were invoked
    verifyCallbacksInvoked();
  }

  private void addAsyncEventQueue(Region region, String name) {
    DiskStoreFactory dsFactory = getCache().createDiskStoreFactory();
    dsFactory.create(name);
    AsyncEventQueueFactory aeqFactory = getCache().createAsyncEventQueueFactory();
    aeqFactory.setDiskStoreName(name);
    aeqFactory.create(name, new CountingAsyncEventListener());
    region.getAttributesMutator().addAsyncEventQueueId(name);
  }

  private void updateRegions() {
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
  }

  private void loadRegions(File dir, SnapshotOptions options) throws Exception {
    RegionGenerator rgen = new RegionGenerator();
    if (options != null) {
      getCache().getSnapshotService().load(dir.listFiles(), SnapshotFormat.GEMFIRE, options);
    } else {
      getCache().getSnapshotService().load(dir, SnapshotFormat.GEMFIRE);
    }
    for (final RegionType rt : RegionType.values()) {
      for (final SerializationType st : SerializationType.values()) {
        Region<Integer, MyObject> region =
            getCache().getRegion("test-" + rt.name() + "-" + st.name());
        for (Entry<Integer, MyObject> entry : createExpected(st, rgen).entrySet()) {
          assertEquals("Comparison failure for " + rt.name() + "/" + st.name(), entry.getValue(),
              region.get(entry.getKey()));
        }
      }
    }
  }

  private void verifyCallbacksInvoked() throws Exception {
    for (final RegionType rt : RegionType.values()) {
      for (final SerializationType st : SerializationType.values()) {
        SerializableCallable counts = new SerializableCallable() {
          @Override
          public Object call() throws Exception {
            String name = "test-" + rt.name() + "-" + st.name();
            Region<Integer, MyObject> region = getCache().getRegion(name);
            // get CacheWriter and CacheListener events
            CountingCacheWriter writer =
                (CountingCacheWriter) region.getAttributes().getCacheWriter();
            CountingCacheListener listener =
                (CountingCacheListener) region.getAttributes().getCacheListener();
            // get AsyncEventListener events
            int numAeqEvents = 0;
            AsyncEventQueue aeq = getCache().getAsyncEventQueue(name);
            CountingAsyncEventListener aeqListener =
                (CountingAsyncEventListener) aeq.getAsyncEventListener();
            if (aeq.isPrimary()) {
              await()
                  .until(() -> aeqListener.getEvents() == NUM_ENTRIES);
              numAeqEvents = aeqListener.getEvents();
            }
            return new int[] {writer.getEvents(), listener.getEvents(), numAeqEvents};
          }
        };
        Object result = forEachVm(counts, true);
        int totalWriterUpdates = 0, totalListenerUpdates = 0, totalAeqEvents = 0;
        List<int[]> list = (List) result;
        for (int[] vmResult : list) {
          totalWriterUpdates += vmResult[0];
          totalListenerUpdates += vmResult[1];
          totalAeqEvents += vmResult[2];
        }
        if (rt.name().contains("PARTITION")) {
          assertEquals(NUM_ENTRIES, totalListenerUpdates);
        } else {
          assertEquals(NUM_ENTRIES * (Host.getHost(0).getVMCount() + 1), totalListenerUpdates);
        }
        assertEquals(NUM_ENTRIES, totalWriterUpdates);
        assertEquals(NUM_ENTRIES, totalAeqEvents);
      }
    }
  }

  public static class SnapshotProblem<K, V> implements SnapshotFilter<K, V> {
    @Override
    public boolean accept(Entry<K, V> entry) {
      throw new RuntimeException();
    }
  };

  @Test
  public void testCacheExportFilterException() throws Exception {
    SnapshotFilter<Object, Object> oops = new SnapshotProblem();

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
        Region<Integer, MyObject> region =
            getCache().getRegion("test-" + rt.name() + "-" + st.name());
        region.putAll(createExpected(st, rgen));
      }
    }
  }

  public static Map<Integer, MyObject> createExpected(SerializationType type,
      RegionGenerator rgen) {
    Map<Integer, MyObject> expected = new HashMap<Integer, MyObject>();
    for (int i = 0; i < NUM_ENTRIES; i++) {
      expected.put(i, rgen.createData(type, i, "The number is " + i));
    }
    return expected;
  }

  public void loadCache() throws Exception {
    SerializableCallable setup = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        CacheFactory cf =
            new CacheFactory().setPdxSerializer(new MyPdxSerializer()).setPdxPersistent(true);

        Cache cache = getCache(cf);
        DiskStore ds = cache.createDiskStoreFactory().setMaxOplogSize(1).setDiskDirs(getDiskDirs())
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
    List result = new ArrayList();
    Host host = Host.getHost(0);
    int vms = host.getVMCount();

    for (int i = 0; i < vms; ++i) {
      result.add(host.getVM(i).invoke(call));
    }

    if (local) {
      result.add(call.call());
    }
    return result;
  }

  private static class CountingCacheListener extends CacheListenerAdapter<Integer, MyObject> {

    private final AtomicInteger events = new AtomicInteger();

    @Override
    public void afterUpdate(EntryEvent<Integer, MyObject> event) {
      events.incrementAndGet();
    }

    private int getEvents() {
      return events.get();
    }
  }

  private static class CountingCacheWriter extends CacheWriterAdapter<Integer, MyObject> {

    private final AtomicInteger events = new AtomicInteger();

    @Override
    public void beforeUpdate(EntryEvent<Integer, MyObject> event) {
      events.incrementAndGet();
    }

    private int getEvents() {
      return events.get();
    }
  }

  private static class CountingAsyncEventListener implements AsyncEventListener {

    private final AtomicInteger events = new AtomicInteger();

    @Override
    public boolean processEvents(final List<AsyncEvent> list) {
      events.addAndGet(list.size());
      return true;
    }

    private int getEvents() {
      return events.get();
    }

    @Override
    public void close() {}
  }
}
