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
package org.apache.geode.internal;

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.pdx.internal.EnumInfo;
import org.apache.geode.pdx.internal.PdxInstanceImpl;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.SerializationTest;

// DHE: comment added to trigger stress new test
@Category({SerializationTest.class})
public class PdxRenameDUnitTest extends JUnit4CacheTestCase {

  final List<String> filesToBeDeleted = new CopyOnWriteArrayList<String>();

  @Test
  public void testPdxRenameVersioning() throws Exception {
    final String DS_NAME = "PdxRenameDUnitTestDiskStore";
    final String DS_NAME2 = "PdxRenameDUnitTestDiskStore2";
    final int[] locatorPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final File f = new File(DS_NAME);
    f.mkdir();
    final File f2 = new File(DS_NAME2);
    f2.mkdir();
    this.filesToBeDeleted.add(DS_NAME);
    this.filesToBeDeleted.add(DS_NAME2);

    final Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS,
        "localhost[" + locatorPorts[0] + "],localhost[" + locatorPorts[1] + "]");
    props.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");

    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        disconnectFromDS();
        props.setProperty(START_LOCATOR, "localhost[" + locatorPorts[0] + "]");
        final Cache cache =
            (new CacheFactory(props)).setPdxPersistent(true).setPdxDiskStore(DS_NAME).create();
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        dsf.setDiskDirs(new File[] {f});
        dsf.create(DS_NAME);
        RegionFactory<String, PdxValue> rf1 =
            cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT);
        rf1.setDiskStoreName(DS_NAME);
        Region<String, PdxValue> region1 = rf1.create("region1");
        region1.put("key1", new PdxValue(1));
        return null;
      }
    });

    vm2.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        disconnectFromDS();
        props.setProperty(START_LOCATOR, "localhost[" + locatorPorts[1] + "]");
        final Cache cache = (new CacheFactory(props)).setPdxReadSerialized(true)
            .setPdxPersistent(true).setPdxDiskStore(DS_NAME2).create();
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        dsf.setDiskDirs(new File[] {f2});
        dsf.create(DS_NAME2);
        RegionFactory rf1 = cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT);
        rf1.setDiskStoreName(DS_NAME2);
        Region region1 = rf1.create("region1");
        Object v = region1.get("key1");
        assertNotNull(v);
        cache.close();
        return null;
      }
    });

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Cache cache = CacheFactory.getAnyInstance();
        if (cache != null && !cache.isClosed()) {
          cache.close();
        }
        return null;
      }
    });

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Collection<Object> renameResults =
            DiskStoreImpl.pdxRename(DS_NAME, new File[] {f}, "apache", "pivotal");
        assertEquals(2, renameResults.size());

        for (Object o : renameResults) {
          if (o instanceof PdxType) {
            PdxType t = (PdxType) o;
            assertEquals("org.pivotal.geode.internal.PdxRenameDUnitTest$PdxValue",
                t.getClassName());
          } else {
            EnumInfo ei = (EnumInfo) o;
            assertEquals("org.pivotal.geode.internal.PdxRenameDUnitTest$Day", ei.getClassName());
          }
        }
        return null;
      }
    });

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        props.setProperty(START_LOCATOR, "localhost[" + locatorPorts[0] + "]");
        final Cache cache =
            (new CacheFactory(props)).setPdxPersistent(true).setPdxDiskStore(DS_NAME).create();
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        dsf.setDiskDirs(new File[] {f});
        dsf.create(DS_NAME);
        RegionFactory<String, PdxValue> rf1 =
            cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT);
        rf1.setDiskStoreName(DS_NAME);
        Region<String, PdxValue> region1 = rf1.create("region1");
        return null;
      }
    });

    vm2.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        disconnectFromDS();
        props.setProperty(START_LOCATOR, "localhost[" + locatorPorts[1] + "]");
        final Cache cache = (new CacheFactory(props)).setPdxReadSerialized(true)
            .setPdxPersistent(true).setPdxDiskStore(DS_NAME2).create();

        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        dsf.setDiskDirs(new File[] {f2});
        dsf.create(DS_NAME2);
        RegionFactory rf1 = cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT);
        rf1.setDiskStoreName(DS_NAME2);
        Region region1 = rf1.create("region1");
        PdxInstance v = (PdxInstance) region1.get("key1");
        assertNotNull(v);
        assertEquals("org.pivotal.geode.internal.PdxRenameDUnitTest$PdxValue",
            ((PdxInstanceImpl) v).getClassName());
        cache.close();
        return null;
      }
    });

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Cache cache = CacheFactory.getAnyInstance();
        if (cache != null && !cache.isClosed()) {
          cache.close();
        }
        return null;
      }
    });
  }

  @Override
  public void preTearDownCacheTestCase() throws Exception {
    for (String path : this.filesToBeDeleted) {
      try {
        Files.delete(new File(path).toPath());
      } catch (IOException e) {
        LogWriterUtils.getLogWriter().error("Unable to delete file", e);
      }
    }
    this.filesToBeDeleted.clear();
  }

  enum Day {
    Sunday, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday;
  }

  class PdxValue implements PdxSerializable {
    private int value;
    public Day aDay;

    public PdxValue(int v) {
      this.value = v;
      aDay = Day.Sunday;
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeInt("value", this.value);
      writer.writeObject("aDay", aDay);
    }

    @Override
    public void fromData(PdxReader reader) {
      this.value = reader.readInt("value");
      this.aDay = (Day) reader.readObject("aDay");
    }
  }
}
