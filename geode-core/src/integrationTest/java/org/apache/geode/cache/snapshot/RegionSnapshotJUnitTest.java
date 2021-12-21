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

import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import com.examples.snapshot.MyObject;
import com.examples.snapshot.MyPdxSerializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.snapshot.RegionGenerator.RegionType;
import org.apache.geode.cache.snapshot.RegionGenerator.SerializationType;
import org.apache.geode.cache.snapshot.SnapshotOptions.SnapshotFormat;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.test.junit.categories.SnapshotTest;

@Category({SnapshotTest.class})
public class RegionSnapshotJUnitTest extends SnapshotTestCase {
  private File snapshotFile;

  @Test
  public void testExportAndReadSnapshot() throws Exception {
    for (final RegionType type : RegionType.values()) {
      for (final SerializationType st : SerializationType.values()) {
        String name = "test-" + type.name() + "-" + st.name();
        Region<Integer, MyObject> region =
            regionGenerator.createRegion(cache, diskStore.getName(), type, name);
        final Map<Integer, MyObject> expected = createExpected(st);

        region.putAll(expected);
        region.getSnapshotService().save(snapshotFile, SnapshotFormat.GEODE);

        final Map<Integer, Object> read = new HashMap<>();
        try (SnapshotIterator<Integer, Object> iter = SnapshotReader.read(snapshotFile)) {
          while (iter.hasNext()) {
            Entry<Integer, Object> entry = iter.next();
            read.put(entry.getKey(), entry.getValue());
          }
          assertEquals("Comparison failure for " + type.name() + "/" + st.name(), expected, read);
        }
      }
    }
  }

  @Test
  public void testExportAndImport() throws Exception {
    for (final RegionType rt : RegionType.values()) {
      for (final SerializationType st : SerializationType.values()) {
        String name = "test-" + rt.name() + "-" + st.name();
        Region<Integer, MyObject> region =
            regionGenerator.createRegion(cache, diskStore.getName(), rt, name);
        final Map<Integer, MyObject> expected = createExpected(st);

        region.putAll(expected);
        region.getSnapshotService().save(snapshotFile, SnapshotFormat.GEODE);

        region.destroyRegion();
        region = regionGenerator.createRegion(cache, diskStore.getName(), rt, name);

        region.getAttributesMutator().setCacheWriter(new CacheWriterAdapter<Integer, MyObject>() {
          @Override
          public void beforeCreate(EntryEvent<Integer, MyObject> event) {
            fail("CacheWriter invoked during import");
          }
        });

        final AtomicBoolean cltest = new AtomicBoolean(false);
        region.getAttributesMutator()
            .addCacheListener(new CacheListenerAdapter<Integer, MyObject>() {
              @Override
              public void afterCreate(EntryEvent<Integer, MyObject> event) {
                cltest.set(true);
              }
            });

        region.getSnapshotService().load(snapshotFile, SnapshotFormat.GEODE);

        assertEquals("Comparison failure for " + rt.name() + "/" + st.name(), expected.entrySet(),
            region.entrySet());
        assertEquals("CacheListener invoked during import", false, cltest.get());
      }
    }
  }

  @Test
  public void testFilterOnExport() throws Exception {
    SnapshotFilter<Integer, MyObject> odd =
        entry -> entry.getKey() % 2 == 1;

    for (final RegionType rt : RegionType.values()) {
      for (final SerializationType st : SerializationType.values()) {
        String name = "test-" + rt.name() + "-" + st.name();
        Region<Integer, MyObject> region =
            regionGenerator.createRegion(cache, diskStore.getName(), rt, name);
        final Map<Integer, MyObject> expected = createExpected(st);

        region.putAll(expected);
        RegionSnapshotService<Integer, MyObject> rss = region.getSnapshotService();
        SnapshotOptions<Integer, MyObject> options = rss.createOptions().setFilter(odd);
        rss.save(snapshotFile, SnapshotFormat.GEODE, options);

        region.destroyRegion();
        region = regionGenerator.createRegion(cache, diskStore.getName(), rt, name);

        rss = region.getSnapshotService();
        rss.load(snapshotFile, SnapshotFormat.GEODE, rss.createOptions());

        region.entrySet().forEach(entry -> assertTrue(odd.accept(entry)));
        assertTrue("Comparison failure for " + rt.name() + "/" + st.name(), region.size() > 0);
      }
    }
  }

  @Test
  public void testFilterOnImport() throws Exception {
    SnapshotFilter<Integer, MyObject> odd =
        entry -> entry.getKey() % 2 == 1;

    for (final RegionType rt : RegionType.values()) {
      for (final SerializationType st : SerializationType.values()) {
        String name = "test-" + rt.name() + "-" + st.name();
        Region<Integer, MyObject> region =
            regionGenerator.createRegion(cache, diskStore.getName(), rt, name);
        final Map<Integer, MyObject> expected = createExpected(st);

        region.putAll(expected);
        RegionSnapshotService<Integer, MyObject> rss = region.getSnapshotService();
        rss.save(snapshotFile, SnapshotFormat.GEODE, rss.createOptions());

        region.destroyRegion();
        region = regionGenerator.createRegion(cache, diskStore.getName(), rt, name);

        rss = region.getSnapshotService();
        SnapshotOptions<Integer, MyObject> options = rss.createOptions().setFilter(odd);
        rss.load(snapshotFile, SnapshotFormat.GEODE, options);

        region.entrySet().forEach(entry -> assertTrue(odd.accept(entry)));
        assertTrue("Comparison failure for " + rt.name() + "/" + st.name(), region.size() > 0);
      }
    }
  }

  @Test
  public void testFilterOnExportAndImport() throws Exception {
    SnapshotFilter<Integer, MyObject> even =
        entry -> entry.getKey() % 2 == 0;

    SnapshotFilter<Integer, MyObject> odd =
        entry -> entry.getKey() % 2 == 1;

    for (final RegionType rt : RegionType.values()) {
      for (final SerializationType st : SerializationType.values()) {
        String name = "test-" + rt.name() + "-" + st.name();
        Region<Integer, MyObject> region =
            regionGenerator.createRegion(cache, diskStore.getName(), rt, name);
        final Map<Integer, MyObject> expected = createExpected(st);

        region.putAll(expected);
        RegionSnapshotService<Integer, MyObject> rss = region.getSnapshotService();
        SnapshotOptions<Integer, MyObject> options = rss.createOptions().setFilter(even);
        rss.save(snapshotFile, SnapshotFormat.GEODE, options);

        region.destroyRegion();
        region = regionGenerator.createRegion(cache, diskStore.getName(), rt, name);

        rss = region.getSnapshotService();
        options = rss.createOptions().setFilter(odd);
        rss.load(snapshotFile, SnapshotFormat.GEODE, options);

        assertEquals("Comparison failure for " + rt.name() + "/" + st.name(), 0, region.size());
      }
    }
  }

  @Test
  public void testFilterExportException() throws Exception {
    SnapshotFilter<Integer, MyObject> oops = entry -> {
      throw new RuntimeException();
    };

    for (final RegionType rt : RegionType.values()) {
      for (final SerializationType st : SerializationType.values()) {
        String name = "test-" + rt.name() + "-" + st.name();
        Region<Integer, MyObject> region =
            regionGenerator.createRegion(cache, diskStore.getName(), rt, name);
        final Map<Integer, MyObject> expected = createExpected(st);

        region.putAll(expected);
        RegionSnapshotService<Integer, MyObject> rss = region.getSnapshotService();
        SnapshotOptions<Integer, MyObject> options = rss.createOptions().setFilter(oops);

        boolean caughtException = false;
        try {
          rss.save(snapshotFile, SnapshotFormat.GEODE, options);
        } catch (RuntimeException e) {
          caughtException = true;
        }
        assertTrue(caughtException);

        region.destroyRegion();
        region = regionGenerator.createRegion(cache, diskStore.getName(), rt, name);

        rss = region.getSnapshotService();
        rss.load(snapshotFile, SnapshotFormat.GEODE, options);

        assertEquals("Comparison failure for " + rt.name() + "/" + st.name(), 0, region.size());
      }
    }
  }

  @Test
  public void testFilterImportException() throws Exception {
    SnapshotFilter<Integer, MyObject> oops = entry -> {
      throw new RuntimeException();
    };

    for (final RegionType rt : RegionType.values()) {
      for (final SerializationType st : SerializationType.values()) {
        String name = "test-" + rt.name() + "-" + st.name();
        Region<Integer, MyObject> region =
            regionGenerator.createRegion(cache, diskStore.getName(), rt, name);
        final Map<Integer, MyObject> expected = createExpected(st);

        region.putAll(expected);
        RegionSnapshotService<Integer, MyObject> rss = region.getSnapshotService();
        rss.save(snapshotFile, SnapshotFormat.GEODE);

        region.destroyRegion();
        region = regionGenerator.createRegion(cache, diskStore.getName(), rt, name);

        rss = region.getSnapshotService();
        SnapshotOptions<Integer, MyObject> options = rss.createOptions().setFilter(oops);

        boolean caughtException = false;
        try {
          rss.load(snapshotFile, SnapshotFormat.GEODE, options);
        } catch (RuntimeException e) {
          caughtException = true;
        }

        assertTrue(caughtException);
        assertEquals("Comparison failure for " + rt.name() + "/" + st.name(), 0, region.size());
      }
    }
  }

  @Test
  public void testInvalidate() throws Exception {
    Region<Integer, MyObject> region =
        regionGenerator.createRegion(cache, diskStore.getName(), RegionType.REPLICATE, "test");
    MyObject obj =
        regionGenerator.createData(SerializationType.SERIALIZABLE, 1, "invalidated value");

    region.put(1, obj);
    region.invalidate(1);

    region.getSnapshotService().save(snapshotFile, SnapshotFormat.GEODE);
    region.getSnapshotService().load(snapshotFile, SnapshotFormat.GEODE);

    assertTrue(region.containsKey(1));
    assertFalse(region.containsValueForKey(1));
    assertNull(region.get(1));
  }

  @Test
  public void testDSID() throws Exception {
    cache.close();

    CacheFactory cf = new CacheFactory().set(MCAST_PORT, "0").set(LOG_LEVEL, "error")
        .setPdxSerializer(new MyPdxSerializer()).set(DISTRIBUTED_SYSTEM_ID, "1");
    cache = cf.create();

    RegionType rt = RegionType.REPLICATE;
    SerializationType st = SerializationType.PDX_SERIALIZER;

    String name = "test-" + rt.name() + "-" + st.name() + "-dsid";
    Region<Integer, MyObject> region =
        regionGenerator.createRegion(cache, diskStore.getName(), rt, name);
    final Map<Integer, MyObject> expected = createExpected(st);

    region.putAll(expected);
    region.getSnapshotService().save(snapshotFile, SnapshotFormat.GEODE);

    cache.close();

    // change the DSID from 1 -> 100
    CacheFactory cf2 = new CacheFactory().set(MCAST_PORT, "0").set(LOG_LEVEL, "error")
        .setPdxSerializer(new MyPdxSerializer()).set(DISTRIBUTED_SYSTEM_ID, "100");
    cache = cf2.create();

    final Map<Integer, Object> read = new HashMap<>();
    try (SnapshotIterator<Integer, Object> iter = SnapshotReader.read(snapshotFile)) {
      while (iter.hasNext()) {
        Entry<Integer, Object> entry = iter.next();
        read.put(entry.getKey(), entry.getValue());
      }
      assertEquals("Comparison failure for " + rt.name() + "/" + st.name(), expected, read);
    }
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    snapshotFile = new File(getSnapshotDirectory(), "test.snapshot.gfd");
  }
}
