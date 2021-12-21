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
import java.util.HashMap;
import java.util.Map;

import com.examples.snapshot.MyObject;
import com.examples.snapshot.MyPdxSerializer;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.snapshot.RegionGenerator.RegionType;
import org.apache.geode.cache.snapshot.RegionGenerator.SerializationType;
import org.apache.geode.cache.snapshot.SnapshotOptions.SnapshotFormat;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.SnapshotTest;

@Category({SnapshotTest.class})
public class SnapshotPerformanceDUnitTest extends JUnit4CacheTestCase {
  public SnapshotPerformanceDUnitTest() {
    super();
  }

  @Test
  public void testPerformance() throws Exception {
    int iterations = 5;
    int dataCount = 10000;

    execute(iterations, dataCount);
  }

  private void execute(int iterations, int dataCount) throws Exception {
    RegionType[] rts = new RegionType[] {RegionType.REPLICATE, RegionType.PARTITION};
    SerializationType[] sts =
        new SerializationType[] {SerializationType.DATA_SERIALIZABLE, SerializationType.PDX};
    for (RegionType rt : rts) {
      for (SerializationType st : sts) {
        for (int i = 0; i < iterations; i++) {
          Region<Integer, MyObject> region = createRegion(rt, st);
          LogWriterUtils.getLogWriter()
              .info("SNP: Testing region " + region.getName() + ", iteration = " + i);

          loadData(region, st, dataCount);
          doExport(region);

          region = createRegion(rt, st);
          doImport(region);
        }
      }
    }
  }

  private void doExport(Region<Integer, MyObject> region) throws Exception {
    File f = new File(getDiskDirs()[0], region.getName() + ".gfd");

    long start = System.currentTimeMillis();
    region.getSnapshotService().save(f, SnapshotFormat.GEODE);
    long elapsed = System.currentTimeMillis() - start;

    int size = region.size();
    long bytes = f.length();

    double eps = 1000.0 * size / elapsed;
    double mbps = 1000.0 * bytes / elapsed / (1024 * 1024);

    LogWriterUtils.getLogWriter()
        .info("SNP: Exported " + size + " entries (" + bytes + " bytes) in " + elapsed + " ms");
    LogWriterUtils.getLogWriter().info("SNP: Export entry rate: " + eps + " entries / sec");
    LogWriterUtils.getLogWriter().info("SNP: Export data rate: " + mbps + " MB / sec");
  }

  private void doImport(Region<Integer, MyObject> region) throws Exception {
    File f = new File(getDiskDirs()[0], region.getName() + ".gfd");

    long start = System.currentTimeMillis();
    region.getSnapshotService().load(f, SnapshotFormat.GEODE);
    long elapsed = System.currentTimeMillis() - start;

    int size = region.size();
    long bytes = f.length();

    double eps = 1000.0 * size / elapsed;
    double mbps = 1000.0 * bytes / elapsed / (1024 * 1024);

    LogWriterUtils.getLogWriter()
        .info("SNP: Imported " + size + " entries (" + bytes + " bytes) in " + elapsed + " ms");
    LogWriterUtils.getLogWriter().info("SNP: Import entry rate: " + eps + " entries / sec");
    LogWriterUtils.getLogWriter().info("SNP: Import data rate: " + mbps + " MB / sec");
  }

  @Override
  public final void postSetUp() throws Exception {
    createCache();
  }

  private void createCache() throws Exception {
    SerializableCallable setup = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        CacheFactory cf = new CacheFactory().setPdxSerializer(new MyPdxSerializer());

        getCache(cf);
        return null;
      }
    };

    SnapshotDUnitTest.forEachVm(setup, true);
  }

  private Region<Integer, MyObject> createRegion(final RegionType rt, final SerializationType st)
      throws Exception {
    final String name = "snapshot-" + rt.name() + "-" + st.name();
    Region<Integer, MyObject> region = getCache().getRegion(name);
    if (region != null) {
      region.destroyRegion();
    }

    SerializableCallable setup = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Cache cache = getCache();
        new RegionGenerator().createRegion(cache, null, rt, name);
        return null;
      }
    };

    SnapshotDUnitTest.forEachVm(setup, true);
    return getCache().getRegion(name);
  }

  private void loadData(Region<Integer, MyObject> region, SerializationType st, int count) {
    RegionGenerator rgen = new RegionGenerator();

    int bufferSize = 1000;
    Map<Integer, MyObject> buffer = new HashMap<>(bufferSize);

    long start = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      buffer.put(i, rgen.createData(st, i,
          "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Mauris at sapien lectus. Nam ullamcorper blandit tempus. Morbi accumsan ornare erat eget lobortis. Mauris laoreet auctor purus et vehicula. Cras hendrerit consectetur odio, in placerat orci vehicula a. Ut laoreet consectetur quam, at pellentesque felis sollicitudin sed. Aliquam imperdiet, augue at vehicula placerat, quam mi feugiat mi, non semper elit diam vitae lectus. Fusce vestibulum erat vitae dui scelerisque aliquet. Nam magna sapien, scelerisque id tincidunt non, dapibus quis ipsum."));
      if (buffer.size() == bufferSize) {
        region.putAll(buffer);
        buffer.clear();
      }
    }

    if (!buffer.isEmpty()) {
      region.putAll(buffer);
    }

    long elapsed = System.currentTimeMillis() - start;
    LogWriterUtils.getLogWriter().info("SNP: loaded " + count + " entries in " + elapsed + " ms");

    assertEquals(count, region.size());
  }
}
