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

import java.io.File;
import java.util.Collection;
import java.util.Properties;
import java.util.regex.Pattern;

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
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.pdx.internal.EnumInfo;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.test.junit.categories.SerializationTest;

@Category({SerializationTest.class})
public class PdxRenameJUnitTest {
  @Test
  public void testGetPdxTypes() throws Exception {
    String DS_NAME = "PdxRenameJUnitTestDiskStore";
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    File f = new File(DS_NAME);
    f.mkdir();
    try {
      final Cache cache =
          (new CacheFactory(props)).setPdxPersistent(true).setPdxDiskStore(DS_NAME).create();
      try {
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        dsf.setDiskDirs(new File[] {f});
        dsf.create(DS_NAME);
        RegionFactory<String, PdxValue> rf1 =
            cache.createRegionFactory(RegionShortcut.LOCAL_PERSISTENT);
        rf1.setDiskStoreName(DS_NAME);
        Region<String, PdxValue> region1 = rf1.create("region1");
        region1.put("key1", new PdxValue(1));
        cache.close();

        Collection<PdxType> types = DiskStoreImpl.getPdxTypes(DS_NAME, new File[] {f});
        assertEquals(1, types.size());
        assertEquals(PdxValue.class.getName(), types.iterator().next().getClassName());
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
  public void testPdxRename() throws Exception {
    String DS_NAME = "PdxRenameJUnitTestDiskStore";
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    File f = new File(DS_NAME);
    f.mkdir();
    try {
      final Cache cache =
          (new CacheFactory(props)).setPdxPersistent(true).setPdxDiskStore(DS_NAME).create();
      try {
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        dsf.setDiskDirs(new File[] {f});
        dsf.create(DS_NAME);
        RegionFactory<String, PdxValue> rf1 =
            cache.createRegionFactory(RegionShortcut.LOCAL_PERSISTENT);
        rf1.setDiskStoreName(DS_NAME);
        Region<String, PdxValue> region1 = rf1.create("region1");
        region1.put("key1", new PdxValue(1));
        cache.close();

        Collection<Object> renameResults =
            DiskStoreImpl.pdxRename(DS_NAME, new File[] {f}, "apache", "pivotal");
        assertEquals(2, renameResults.size());

        for (Object o : renameResults) {
          if (o instanceof PdxType) {
            PdxType t = (PdxType) o;
            assertEquals("org.pivotal.geode.internal.PdxRenameJUnitTest$PdxValue",
                t.getClassName());
          } else {
            EnumInfo ei = (EnumInfo) o;
            assertEquals("org.pivotal.geode.internal.PdxRenameJUnitTest$Day", ei.getClassName());
          }
        }
        Collection<PdxType> types = DiskStoreImpl.getPdxTypes(DS_NAME, new File[] {f});
        assertEquals(1, types.size());
        assertEquals("org.pivotal.geode.internal.PdxRenameJUnitTest$PdxValue",
            types.iterator().next().getClassName());

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
  public void testRegEx() {
    Pattern pattern = DiskStoreImpl.createPdxRenamePattern("foo");
    assertEquals(null, DiskStoreImpl.replacePdxRenamePattern(pattern, "", "FOOBAR"));
    assertEquals(null, DiskStoreImpl.replacePdxRenamePattern(pattern, "afoob", "FOOBAR"));
    assertEquals(null, DiskStoreImpl.replacePdxRenamePattern(pattern, "foob", "FOOBAR"));
    assertEquals(null, DiskStoreImpl.replacePdxRenamePattern(pattern, "afoob", "FOOBAR"));
    assertEquals("bar", DiskStoreImpl.replacePdxRenamePattern(pattern, "foo", "bar"));
    assertEquals(".bar", DiskStoreImpl.replacePdxRenamePattern(pattern, ".foo", "bar"));
    assertEquals("bar.", DiskStoreImpl.replacePdxRenamePattern(pattern, "foo.", "bar"));
    assertEquals("Class$bar", DiskStoreImpl.replacePdxRenamePattern(pattern, "Class$foo", "bar"));
    assertEquals("Class$showMeThe$1.",
        DiskStoreImpl.replacePdxRenamePattern(pattern, "Class$foo.", "showMeThe$1"));
    pattern = DiskStoreImpl.createPdxRenamePattern("foo.bar");
    assertEquals("com.pivotal.Hello",
        DiskStoreImpl.replacePdxRenamePattern(pattern, "com.foo.bar.Hello", "pivotal"));
  }

  enum Day {
    Sunday, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday
  }

  class PdxValue implements PdxSerializable {
    private int value;
    public Day aDay;

    public PdxValue(int v) {
      value = v;
      aDay = Day.Sunday;
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeInt("value", value);
      writer.writeObject("aDay", aDay);
    }

    @Override
    public void fromData(PdxReader reader) {
      value = reader.readInt("value");
      aDay = (Day) reader.readObject("aDay");
    }
  }
}
