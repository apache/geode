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

import org.junit.experimental.categories.Category;
import org.junit.Test;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import com.examples.snapshot.MyPdxSerializer;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.snapshot.RegionGenerator.RegionType;
import org.apache.geode.cache.snapshot.SnapshotOptions.SnapshotFormat;
import org.apache.geode.internal.cache.snapshot.SnapshotOptionsImpl;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;

@Category(DistributedTest.class)
public class ParallelSnapshotDUnitTest extends JUnit4CacheTestCase {
  private static final byte[] ffff = new byte[] {0xf, 0xf, 0xf, 0xf};
  private static final byte[] eeee = new byte[] {0xe, 0xe, 0xe, 0xe};

  @Test
  public void testExportImport() throws Exception {
    doExport(false);
    doImport(false);
  }

  @Test
  public void testExportWithSequentialImport() throws Exception {
    doExport(false);
    doSequentialImport();
  }

  @Test
  public void testExportImportErrors() throws Exception {
    try {
      doExport(true);
      fail();
    } catch (Exception e) {
    }

    doExport(false);
    try {
      doImport(true);
      fail();
    } catch (Exception e) {
    }
  }

  private void doExport(boolean explode) throws Exception {
    Region region = getCache().getRegion("test");
    for (int i = 0; i < 1000; i++) {
      region.put(i, ffff);
    }

    RegionSnapshotService rss = region.getSnapshotService();

    final TestSnapshotFileMapper mapper = new TestSnapshotFileMapper();
    mapper.setShouldExplode(explode);

    SnapshotOptionsImpl opt = (SnapshotOptionsImpl) rss.createOptions();
    opt.setParallelMode(true);
    opt.setMapper(mapper);

    File f = new File("mysnap.gfd").getAbsoluteFile();
    rss.save(f, SnapshotFormat.GEMFIRE, opt);

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

    forEachVm(check, true);
  }

  private void doImport(boolean explode) throws ClassNotFoundException, IOException {
    Region region = getCache().getRegion("test");
    RegionSnapshotService rss = region.getSnapshotService();

    final TestSnapshotFileMapper mapper = new TestSnapshotFileMapper();
    mapper.setShouldExplode(explode);

    SnapshotOptionsImpl opt = (SnapshotOptionsImpl) rss.createOptions();
    opt.setParallelMode(true);
    opt.setMapper(mapper);

    final File f = new File("mysnap.gfd").getAbsoluteFile();

    for (int i = 0; i < 1000; i++) {
      region.put(i, eeee);
    }

    rss.load(f, SnapshotFormat.GEMFIRE, opt);
    for (int i = 0; i < 1000; i++) {
      assertTrue(Arrays.equals(ffff, (byte[]) region.get(i)));
    }
  }

  private void doSequentialImport() throws IOException, ClassNotFoundException {
    Region region = getCache().getRegion("test");
    RegionSnapshotService rss = region.getSnapshotService();
    SnapshotOptionsImpl opt = (SnapshotOptionsImpl) rss.createOptions();


    for (int i = 0; i < 1000; i++) {
      region.put(i, eeee);
    }

    final File file = new File("").getAbsoluteFile();
    rss.load(file, SnapshotFormat.GEMFIRE, opt);
    for (int i = 0; i < 1000; i++) {
      assertTrue(Arrays.equals(ffff, (byte[]) region.get(i)));
    }
  }

  public Object forEachVm(SerializableCallable call, boolean local) throws Exception {
    Host host = Host.getHost(0);
    int vms = host.getVMCount();

    for (int i = 0; i < vms; ++i) {
      host.getVM(i).invoke(call);
    }

    if (local) {
      return call.call();
    }
    return null;
  }

  @Override
  public final void postSetUp() throws Exception {
    loadCache();
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    File[] snaps = new File(".").listFiles((dir, name) -> name.startsWith("mysnap"));

    if (snaps != null) {
      for (File f : snaps) {
        f.delete();
      }
    }
  }

  public void loadCache() throws Exception {
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

    forEachVm(setup, true);
  }
}
