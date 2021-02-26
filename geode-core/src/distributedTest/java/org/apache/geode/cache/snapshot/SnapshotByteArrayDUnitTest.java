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

import java.io.File;

import com.examples.snapshot.MyPdxSerializer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.snapshot.RegionGenerator.RegionType;
import org.apache.geode.cache.snapshot.SnapshotOptions.SnapshotFormat;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.SnapshotTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

@Category({SnapshotTest.class})
public class SnapshotByteArrayDUnitTest extends JUnit4CacheTestCase {
  @Rule
  public TemporaryFolder tempDir = new SerializableTemporaryFolder();

  private File snap;

  @Before
  public void setup() throws Exception {
    snap = new File(tempDir.getRoot(), "snapshot-ops.gfd");
    loadCache();
  }

  @Test
  public void testImportByteArray() throws Exception {
    SerializableCallable load = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region region = getCache().getRegion("snapshot-ops");
        for (int i = 0; i < 1000; i++) {
          region.put(i, new byte[] {0xf});
        }

        region.getSnapshotService().save(snap, SnapshotFormat.GEODE);
        region.getSnapshotService().load(snap, SnapshotFormat.GEODE);

        return null;
      }
    };

    Host.getHost(0).getVM(1).invoke(load);

    SerializableCallable callback = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region region = getCache().getRegion("snapshot-ops");
        region.getAttributesMutator().addCacheListener(new CacheListenerAdapter<Integer, Object>() {
          @Override
          public void afterUpdate(EntryEvent<Integer, Object> event) {
            dump(event);
          }

          @Override
          public void afterInvalidate(EntryEvent<Integer, Object> event) {
            dump(event);
          }

          @Override
          public void afterDestroy(EntryEvent<Integer, Object> event) {
            dump(event);
          }

          @Override
          public void afterCreate(EntryEvent<Integer, Object> event) {}

          private void dump(EntryEvent<Integer, Object> event) {
            LogWriterUtils.getLogWriter().info("op = " + event.getOperation());

            Object obj1 = event.getNewValue();
            LogWriterUtils.getLogWriter().info("new = " + obj1);

            Object obj2 = event.getOldValue();
            LogWriterUtils.getLogWriter().info("old = " + obj2);
          }
        });

        return null;
      }
    };

    SnapshotDUnitTest.forEachVm(callback, true);
    Region region = getCache().getRegion("snapshot-ops");

    for (int i = 0; i < 1000; i++) {
      region.put(i, new byte[] {0x0, 0x1, 0x3});
      region.invalidate(i);
      region.destroy(i);
    }
  }

  private void loadCache() throws Exception {
    SerializableCallable setup = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        CacheFactory cf =
            new CacheFactory().setPdxSerializer(new MyPdxSerializer()).setPdxPersistent(true);

        Cache cache = getCache(cf);
        RegionGenerator rgen = new RegionGenerator();
        rgen.createRegion(cache, null, RegionType.REPLICATE, "snapshot-ops");

        return null;
      }
    };

    SnapshotDUnitTest.forEachVm(setup, true);
  }
}
