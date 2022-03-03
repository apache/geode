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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.io.File;
import java.util.Collection;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.util.BlobHelper;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.pdx.internal.PdxField;
import org.apache.geode.pdx.internal.PdxInstanceImpl;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.pdx.internal.PdxUnreadData;
import org.apache.geode.pdx.internal.TypeRegistry;
import org.apache.geode.test.junit.categories.SerializationTest;

@Category({SerializationTest.class})
public class PdxDeleteFieldJUnitTest {

  @Test
  public void testPdxDeleteField() throws Exception {
    String DS_NAME = "PdxDeleteFieldJUnitTestDiskStore";
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    File f = new File(DS_NAME);
    f.mkdir();
    try {
      Cache cache =
          (new CacheFactory(props)).setPdxPersistent(true).setPdxDiskStore(DS_NAME).create();
      try {
        {
          DiskStoreFactory dsf = cache.createDiskStoreFactory();
          dsf.setDiskDirs(new File[] {f});
          dsf.create(DS_NAME);
        }
        RegionFactory<String, PdxValue> rf1 =
            cache.createRegionFactory(RegionShortcut.LOCAL_PERSISTENT);
        rf1.setDiskStoreName(DS_NAME);
        Region<String, PdxValue> region1 = rf1.create("region1");
        PdxValue pdxValue = new PdxValue(1, 2L);
        region1.put("key1", pdxValue);
        byte[] pdxValueBytes = BlobHelper.serializeToBlob(pdxValue);
        {
          PdxValue deserializedPdxValue = (PdxValue) BlobHelper.deserializeBlob(pdxValueBytes);
          assertEquals(1, deserializedPdxValue.value);
          assertEquals(2L, deserializedPdxValue.fieldToDelete);
        }
        cache.close();

        Collection<PdxType> types = DiskStoreImpl.pdxDeleteField(DS_NAME, new File[] {f},
            PdxValue.class.getName(), "fieldToDelete");
        assertEquals(1, types.size());
        PdxType pt = types.iterator().next();
        assertEquals(PdxValue.class.getName(), pt.getClassName());
        assertEquals(null, pt.getPdxField("fieldToDelete"));
        types = DiskStoreImpl.getPdxTypes(DS_NAME, new File[] {f});
        assertEquals(1, types.size());
        pt = types.iterator().next();
        assertEquals(PdxValue.class.getName(), pt.getClassName());
        assertEquals(true, pt.getHasDeletedField());
        assertEquals(null, pt.getPdxField("fieldToDelete"));

        cache = (new CacheFactory(props)).setPdxPersistent(true).setPdxDiskStore(DS_NAME).create();
        {
          DiskStoreFactory dsf = cache.createDiskStoreFactory();
          dsf.setDiskDirs(new File[] {f});
          dsf.create(DS_NAME);
          PdxValue deserializedPdxValue = (PdxValue) BlobHelper.deserializeBlob(pdxValueBytes);
          assertEquals(1, deserializedPdxValue.value);
          assertEquals(0L, deserializedPdxValue.fieldToDelete);
        }
      } finally {
        if (!cache.isClosed()) {
          cache.close();
        }
      }
    } finally {
      FileUtils.deleteDirectory(f);
    }
  }

  @Test
  public void testPdxFieldDelete() throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    try {
      InternalCache cache = (InternalCache) new CacheFactory(props).create();
      try {
        PdxValue pdxValue = new PdxValue(1, 2L);
        byte[] pdxValueBytes = BlobHelper.serializeToBlob(pdxValue);
        {
          PdxValue deserializedPdxValue = (PdxValue) BlobHelper.deserializeBlob(pdxValueBytes);
          assertEquals(1, deserializedPdxValue.value);
          assertEquals(2L, deserializedPdxValue.fieldToDelete);
        }
        PdxType pt;
        cache.setPdxReadSerializedOverride(true);
        try {
          PdxInstanceImpl pi = (PdxInstanceImpl) BlobHelper.deserializeBlob(pdxValueBytes);
          pt = pi.getPdxType();
          assertEquals(1, pi.getField("value"));
          assertEquals(2L, pi.getField("fieldToDelete"));
        } finally {
          cache.setPdxReadSerializedOverride(false);
        }
        assertEquals(PdxValue.class.getName(), pt.getClassName());
        PdxField field = pt.getPdxField("fieldToDelete");
        pt.setHasDeletedField(true);
        field.setDeleted(true);
        assertEquals(null, pt.getPdxField("fieldToDelete"));
        assertEquals(2, pt.getFieldCount());

        {
          PdxValue deserializedPdxValue = (PdxValue) BlobHelper.deserializeBlob(pdxValueBytes);
          assertEquals(1, deserializedPdxValue.value);
          // fieldToDelete should now be 0 (the default) instead of 2.
          assertEquals(0L, deserializedPdxValue.fieldToDelete);
        }
        cache.setPdxReadSerializedOverride(true);
        try {
          PdxInstance pi = (PdxInstance) BlobHelper.deserializeBlob(pdxValueBytes);
          assertEquals(1, pi.getField("value"));
          assertEquals(false, pi.hasField("fieldToDelete"));
          assertEquals(null, pi.getField("fieldToDelete"));
          assertSame(pt, ((PdxInstanceImpl) pi).getPdxType());
          PdxValue deserializedPdxValue = (PdxValue) pi.getObject();
          assertEquals(1, deserializedPdxValue.value);
          assertEquals(0L, deserializedPdxValue.fieldToDelete);
        } finally {
          cache.setPdxReadSerializedOverride(false);
        }
        TypeRegistry tr = cache.getPdxRegistry();
        // Clear the local registry so we will regenerate a type for the same class
        tr.testClearLocalTypeRegistry();
        {
          PdxInstanceFactory piFactory = cache.createPdxInstanceFactory(PdxValue.class.getName());
          piFactory.writeInt("value", 1);
          PdxInstance pi = piFactory.create();
          assertEquals(1, pi.getField("value"));
          assertEquals(null, pi.getField("fieldToDelete"));
          PdxType pt2 = ((PdxInstanceImpl) pi).getPdxType();
          assertEquals(null, pt2.getPdxField("fieldToDelete"));
          assertEquals(1, pt2.getFieldCount());
        }

      } finally {
        if (!cache.isClosed()) {
          cache.close();
        }
      }
    } finally {
    }
  }

  public static class PdxValue implements PdxSerializable {
    public int value;
    public long fieldToDelete = -1L;

    public PdxValue() {} // for deserialization

    public PdxValue(int v, long lv) {
      value = v;
      fieldToDelete = lv;
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeInt("value", value);
      writer.writeLong("fieldToDelete", fieldToDelete);
    }

    @Override
    public void fromData(PdxReader reader) {
      value = reader.readInt("value");
      if (reader.hasField("fieldToDelete")) {
        fieldToDelete = reader.readLong("fieldToDelete");
      } else {
        fieldToDelete = 0L;
        PdxUnreadData unread = (PdxUnreadData) reader.readUnreadFields();
        assertEquals(true, unread.isEmpty());
      }
    }
  }
}
