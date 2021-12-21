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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import com.examples.snapshot.MyPdxSerializer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.snapshot.RegionGenerator.RegionType;
import org.apache.geode.cache.snapshot.SnapshotOptions.SnapshotFormat;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.cache.snapshot.SnapshotOptionsImpl;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.SnapshotTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

@Category({SnapshotTest.class})
public class ParallelSnapshotDUnitTest extends JUnit4CacheTestCase {
  private static final byte[] ffff = new byte[] {0xf, 0xf, 0xf, 0xf};
  private static final byte[] eeee = new byte[] {0xe, 0xe, 0xe, 0xe};
  private static final int DATA_POINTS = 100;

  private File directory;

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Before
  public void setup() throws IOException {
    directory = temporaryFolder.newFolder();
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties properties = super.getDistributedSystemProperties();
    properties.put(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        TestSnapshotFileMapper.class.getName());
    return properties;
  }


  @Test
  public void testExportImport() throws Exception {
    loadCache();
    doExport(false);
    doImport(false);
  }

  @Test
  public void testExportWithSequentialImport() throws Exception {
    loadCache();
    doExport(false);
    doSequentialImport();
  }

  @Test
  public void testExportImportErrors() throws Exception {
    loadCache();
    try {
      doExport(true);
      fail("Expected exception not thrown");
    } catch (Exception e) {
      // do nothing on expected exception from test
    }

    doExport(false);
    try {
      doImport(true);
      fail("Expected exception not thrown");
    } catch (Exception e) {
      // do nothing on expected exception from test
    }
  }

  /**
   * This test ensures that parallel import succeeds even when each node does not have a file to
   * import (import cluster larger than export one)
   *
   */
  @Test
  public void testImportOnLargerCluster() throws Exception {
    loadCache(2);
    doExport(false, 2);
    getCache().getRegion("test").destroyRegion();
    loadCache();
    doImport(false);
  }

  private void doExport(boolean explode) throws Exception {
    doExport(explode, Host.getHost(0).getVMCount());
  }

  private void doExport(boolean explode, int nodes) throws Exception {
    Region region = getCache().getRegion("test");
    for (int i = 0; i < DATA_POINTS; i++) {
      region.put(i, ffff);
    }

    RegionSnapshotService rss = region.getSnapshotService();

    final TestSnapshotFileMapper mapper = new TestSnapshotFileMapper();
    mapper.setShouldExplode(explode);

    SnapshotOptionsImpl opt = (SnapshotOptionsImpl) rss.createOptions();
    opt.setParallelMode(true);
    opt.setMapper(mapper);

    File f = new File(directory, "mysnap.gfd").getAbsoluteFile();
    rss.save(f, SnapshotFormat.GEODE, opt);

    mapper.setShouldExplode(false);
    SerializableCallable check = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        getCache().getDistributedSystem().getDistributedMember();
        File snap =
            mapper.mapExportPath(getCache().getDistributedSystem().getDistributedMember(), f);
        assertTrue("Could not find snapshot: " + snap, snap.exists());
        return null;
      }
    };

    forEachVm(check, true, nodes);
  }

  private void doImport(boolean explode) throws ClassNotFoundException, IOException {
    Region region = getCache().getRegion("test");
    RegionSnapshotService rss = region.getSnapshotService();

    final TestSnapshotFileMapper mapper = new TestSnapshotFileMapper();
    mapper.setShouldExplode(explode);

    SnapshotOptionsImpl opt = (SnapshotOptionsImpl) rss.createOptions();
    opt.setParallelMode(true);
    opt.setMapper(mapper);

    for (int i = 0; i < DATA_POINTS; i++) {
      region.put(i, eeee);
    }

    rss.load(directory, SnapshotFormat.GEODE, opt);
    for (int i = 0; i < DATA_POINTS; i++) {
      assertTrue(Arrays.equals(ffff, (byte[]) region.get(i)));
    }
  }

  private void doSequentialImport() throws IOException, ClassNotFoundException {
    Region region = getCache().getRegion("test");
    RegionSnapshotService rss = region.getSnapshotService();
    SnapshotOptionsImpl opt = (SnapshotOptionsImpl) rss.createOptions();


    for (int i = 0; i < DATA_POINTS; i++) {
      region.put(i, eeee);
    }
    int vmCount = Host.getHost(0).getVMCount();
    for (int i = 0; i <= vmCount; i++) {
      rss.load(new File(directory, Integer.toString(i)), SnapshotFormat.GEODE, opt);
    }
    for (int i = 0; i < DATA_POINTS; i++) {
      assertTrue(Arrays.equals(ffff, (byte[]) region.get(i)));
    }
  }

  private void forEachVm(SerializableCallable call, boolean local, int maxNodes) throws Exception {
    Host host = Host.getHost(0);
    int vms = Math.min(host.getVMCount(), maxNodes);

    for (int i = 0; i < vms; ++i) {
      host.getVM(i).invoke(call);
    }

    if (local) {
      call.call();
    }
  }

  @Override
  public final void postSetUp() throws Exception {}

  private void loadCache() throws Exception {
    loadCache(Integer.MAX_VALUE);
  }

  private void loadCache(int maxNodes) throws Exception {
    SerializableCallable setup = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        CacheFactory cf = new CacheFactory().setPdxSerializer(new MyPdxSerializer());

        Cache cache = getCache(cf);
        RegionGenerator rgen = new RegionGenerator();
        rgen.createRegion(cache, null, RegionType.PARTITION, "test");

        return null;
      }
    };

    forEachVm(setup, true, maxNodes);
  }
}
