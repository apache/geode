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
package com.gemstone.gemfire.internal;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxSerializable;
import com.gemstone.gemfire.pdx.PdxWriter;
import com.gemstone.gemfire.pdx.internal.PdxType;
import com.gemstone.gemfire.pdx.internal.PdxUnreadData;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;

public class PdxDeleteFieldDUnitTest  extends CacheTestCase{
  final List<String> filesToBeDeleted = new CopyOnWriteArrayList<String>();
  
  public PdxDeleteFieldDUnitTest(String name) {
    super(name);
  }
  
  public void testPdxDeleteFieldVersioning() throws Exception {
    final String DS_NAME = "PdxDeleteFieldDUnitTestDiskStore";
    final String DS_NAME2 = "PdxDeleteFieldDUnitTestDiskStore2";
    
    final Properties props = new Properties();
    final int[] locatorPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost["+locatorPorts[0]+"],localhost["+locatorPorts[1]+"]");
    props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");

    final File f = new File(DS_NAME);
    f.mkdir();
    final File f2 = new File(DS_NAME2);
    f2.mkdir();
    this.filesToBeDeleted.add(DS_NAME);
    this.filesToBeDeleted.add(DS_NAME2);
    
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);
    
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        disconnectFromDS();
        props.setProperty(DistributionConfig.START_LOCATOR_NAME, "localhost["+locatorPorts[0]+"]");
        final Cache cache = (new CacheFactory(props)).setPdxPersistent(true).setPdxDiskStore(DS_NAME).create();
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        dsf.setDiskDirs(new File[]{f});
        dsf.create(DS_NAME);
        RegionFactory<String, PdxValue> rf1 = cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT);    
        rf1.setDiskStoreName(DS_NAME);
        Region<String, PdxValue> region1 = rf1.create("region1");
        region1.put("key1", new PdxValue(1, 2L));
        return null;
      }
    });
    
    vm2.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        disconnectFromDS();
        props.setProperty(DistributionConfig.START_LOCATOR_NAME, "localhost["+locatorPorts[1]+"]");
        final Cache cache = (new CacheFactory(props)).setPdxReadSerialized(true).setPdxPersistent(true).setPdxDiskStore(DS_NAME2).create();
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        dsf.setDiskDirs(new File[]{f2});
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
      public Object call() throws Exception {
        Cache cache = CacheFactory.getAnyInstance();
        if(cache != null && !cache.isClosed()) {
          cache.close();
        }
        return null;
      }
    });
    
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Collection<PdxType> types = DiskStoreImpl.pdxDeleteField(DS_NAME, new File[]{f}, PdxValue.class.getName(), "fieldToDelete");
        assertEquals(1, types.size());
        PdxType pt = types.iterator().next();
        assertEquals(PdxValue.class.getName(), pt.getClassName());
        assertEquals(null, pt.getPdxField("fieldToDelete"));
        return null;
      }
    });
    
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        props.setProperty(DistributionConfig.START_LOCATOR_NAME, "localhost["+locatorPorts[0]+"]");
        final Cache cache = (new CacheFactory(props)).setPdxPersistent(true).setPdxDiskStore(DS_NAME).create();
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        dsf.setDiskDirs(new File[]{f});
        dsf.create(DS_NAME);
        RegionFactory<String, PdxValue> rf1 = cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT);    
        rf1.setDiskStoreName(DS_NAME);
        Region<String, PdxValue> region1 = rf1.create("region1");
        return null;
      }
    });
    
    vm2.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        props.setProperty(DistributionConfig.START_LOCATOR_NAME, "localhost["+locatorPorts[1]+"]");
        final Cache cache = (new CacheFactory(props)).setPdxReadSerialized(true).setPdxPersistent(true).setPdxDiskStore(DS_NAME2).create();
        
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        dsf.setDiskDirs(new File[]{f2});
        dsf.create(DS_NAME2);
        RegionFactory rf1 = cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT);    
        rf1.setDiskStoreName(DS_NAME2);
        Region region1 = rf1.create("region1");
        PdxInstance v = (PdxInstance) region1.get("key1");
        assertNotNull(v);
        assertEquals(1, v.getField("value"));
        assertEquals(null, v.getField("fieldToDelete"));
        cache.close();
        return null;
      }
    });
    
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Cache cache = CacheFactory.getAnyInstance();
        if(cache != null && !cache.isClosed()) {
          cache.close();
        }
        return null;
      }
    });
  }

  @Override
  public void tearDown2() throws Exception {
    for (String path : this.filesToBeDeleted) {
      try {
        FileUtil.delete(new File(path));
      } catch (IOException e) {
        getLogWriter().error("Unable to delete file", e);
      }
    }
    this.filesToBeDeleted.clear();
    super.tearDown2();
  }
  
  public static class PdxValue implements PdxSerializable {
    public int value;
    public long fieldToDelete = -1L;
    public PdxValue() {} // for deserialization
    public PdxValue(int v, long lv) {
      this.value = v;
      this.fieldToDelete = lv;
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeInt("value", this.value);
      writer.writeLong("fieldToDelete", this.fieldToDelete);
    }

    @Override
    public void fromData(PdxReader reader) {
      this.value = reader.readInt("value");
      if (reader.hasField("fieldToDelete")) {
        this.fieldToDelete = reader.readLong("fieldToDelete");
      } else {
        this.fieldToDelete = 0L;
        PdxUnreadData unread = (PdxUnreadData) reader.readUnreadFields();
        assertEquals(true, unread.isEmpty());
      }
    }
  }
}
