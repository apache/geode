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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Map.Entry;

import com.examples.snapshot.MyObject;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.snapshot.RegionGenerator.RegionType;
import org.apache.geode.cache.snapshot.RegionGenerator.SerializationType;
import org.apache.geode.cache.snapshot.SnapshotOptions.SnapshotFormat;
import org.apache.geode.test.junit.categories.SnapshotTest;

@Category({SnapshotTest.class})
public class CacheSnapshotJUnitTest extends SnapshotTestCase {
  @Test
  public void testExportAndImport() throws Exception {
    for (final RegionType rt : RegionType.values()) {
      for (final SerializationType st : SerializationType.values()) {
        Region<Integer, MyObject> region = regionGenerator.createRegion(cache, diskStore.getName(),
            rt, "test-" + rt.name() + "-" + st.name());
        region.putAll(createExpected(st));
      }
    }

    // save all regions
    cache.getSnapshotService().save(getSnapshotDirectory(), SnapshotFormat.GEODE);

    for (final RegionType rt : RegionType.values()) {
      for (final SerializationType st : SerializationType.values()) {
        String name = "test-" + rt.name() + "-" + st.name();

        Region<Integer, MyObject> region = cache.getRegion(name);
        region.destroyRegion();

        regionGenerator.createRegion(cache, diskStore.getName(), rt, name);
      }
    }

    // load all regions
    cache.getSnapshotService().load(getSnapshotDirectory(), SnapshotFormat.GEODE);

    for (final RegionType rt : RegionType.values()) {
      for (final SerializationType st : SerializationType.values()) {
        Region<Integer, MyObject> region = cache.getRegion("test-" + rt.name() + "-" + st.name());
        for (Entry<Integer, MyObject> entry : createExpected(st).entrySet()) {
          assertEquals("Comparison failure for " + rt.name() + "/" + st.name(), entry.getValue(),
              region.get(entry.getKey()));
        }
      }
    }
  }

  @Test
  public void testFilter() throws Exception {
    for (final RegionType rt : RegionType.values()) {
      for (final SerializationType st : SerializationType.values()) {
        Region<Integer, MyObject> region = regionGenerator.createRegion(cache, diskStore.getName(),
            rt, "test-" + rt.name() + "-" + st.name());
        region.putAll(createExpected(st));
      }
    }

    SnapshotFilter<Object, Object> even =
        (SnapshotFilter<Object, Object>) entry -> ((Integer) entry.getKey()) % 2 == 0;

    SnapshotFilter<Object, Object> odd =
        (SnapshotFilter<Object, Object>) entry -> ((Integer) entry.getKey()) % 2 == 1;

    // save even entries
    CacheSnapshotService css = cache.getSnapshotService();
    SnapshotOptions<Object, Object> options = css.createOptions().setFilter(even);
    cache.getSnapshotService().save(getSnapshotDirectory(), SnapshotFormat.GEODE, options);

    for (final RegionType rt : RegionType.values()) {
      for (final SerializationType st : SerializationType.values()) {
        Region region = cache.getRegion("test-" + rt.name() + "-" + st.name());
        region.destroyRegion();
        regionGenerator.createRegion(cache, diskStore.getName(), rt,
            "test-" + rt.name() + "-" + st.name());
      }
    }

    // load odd entries
    File[] snapshots =
        getSnapshotDirectory().listFiles(pathname -> pathname.getName().startsWith("snapshot-"));

    options = css.createOptions().setFilter(odd);
    css.load(snapshots, SnapshotFormat.GEODE, options);

    for (final RegionType rt : RegionType.values()) {
      for (final SerializationType st : SerializationType.values()) {
        Region region = cache.getRegion("test-" + rt.name() + "-" + st.name());
        assertEquals("Comparison failure for " + rt.name() + "/" + st.name(), 0, region.size());
      }
    }
  }
}
