/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.pdx;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializer;
import org.apache.geode.ToDataException;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.pdx.SimpleClass.SimpleEnum;
import org.apache.geode.pdx.internal.EnumId;
import org.apache.geode.pdx.internal.EnumInfo;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.pdx.internal.PeerTypeRegistration;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.SerializationTest;

@Category(SerializationTest.class)
public class PdxAttributesDistributedTest extends JUnit4CacheTestCase {

  private File diskDir;

  private Cache cache;

  @Before
  public void setUp() {
    diskDir = new File("PdxAttributesJUnitTest");
    GemFireCacheImpl.setDefaultDiskStoreName("PDXAttributesDefault");
    diskDir.mkdirs();
  }

  @After
  public void tearDown() throws Exception {
    GemFireCacheImpl instance = (GemFireCacheImpl) basicGetCache();
    if (instance != null) {
      instance.close();
    }
    if (cache != null) {
      cache.close();
    }
    FileUtils.deleteDirectory(diskDir);
    File[] defaultStoreFiles = new File(".").listFiles(
        (dir, name) -> name.startsWith("BACKUPPDXAttributes"));

    for (File file : defaultStoreFiles) {
      FileUtils.forceDelete(file);
    }
  }

  @Test
  public void testPdxPersistent() throws Exception {
    {
      initCache(false, false, null);
      // define a type
      defineAType();
      Region pdxRegion = cache.getRegion(PeerTypeRegistration.REGION_NAME);
      assertEquals(DataPolicy.REPLICATE, pdxRegion.getAttributes().getDataPolicy());
    }

    tearDown();
    setUp();

    {
      initCache(false, true, null);
      // define a type
      defineAType();
      Region pdxRegion = cache.getRegion(PeerTypeRegistration.REGION_NAME);
      assertEquals(DataPolicy.PERSISTENT_REPLICATE, pdxRegion.getAttributes().getDataPolicy());
    }
  }

  @Test
  public void testPdxTypeId() throws Exception {
    int dsId = 5;
    initCache(false, false, String.valueOf(dsId));
    // define a type.
    defineAType();

    Region pdxRegion = cache.getRegion(PeerTypeRegistration.REGION_NAME);
    Iterator itr = pdxRegion.entrySet().iterator();

    boolean found = false;
    boolean foundEnum = false;
    while (itr.hasNext()) {
      Map.Entry ent = (Map.Entry) itr.next();
      if (ent.getKey() instanceof Integer) {
        int pdxTypeId = (int) ent.getKey();
        PdxType pdxType = (PdxType) ent.getValue();

        int pdxTypeHashcode = pdxType.hashCode();
        System.out.println("pdx hashcode " + pdxTypeHashcode);
        int expectedPdxTypeId =
            (dsId << 24) | (PeerTypeRegistration.PLACE_HOLDER_FOR_TYPE_ID & pdxTypeHashcode);

        assertEquals(expectedPdxTypeId, pdxTypeId);

        found = true;
      } else {
        EnumId enumId = (EnumId) ent.getKey();
        EnumInfo enumInfo = (EnumInfo) ent.getValue();

        EnumInfo expectedEnumInfo = new EnumInfo(SimpleEnum.TWO);
        int expectKey = (dsId << 24)
            | (PeerTypeRegistration.PLACE_HOLDER_FOR_TYPE_ID & expectedEnumInfo.hashCode());;

        assertEquals(expectKey, enumId.intValue());
        foundEnum = true;
      }
    }

    assertEquals(true, found);
    assertEquals(true, foundEnum);
    cache.close();

  }

  @Test
  public void testDuplicatePdxTypeId() throws Exception {
    int dsId = 5;
    initCache(false, false, String.valueOf(dsId));
    // define a type.
    defineAType();

    Region pdxRegion = cache.getRegion(PeerTypeRegistration.REGION_NAME);
    Iterator itr = pdxRegion.entrySet().iterator();

    boolean foundException = false;
    boolean foundEnumException = false;
    while (itr.hasNext()) {
      Map.Entry ent = (Map.Entry) itr.next();
      if (ent.getKey() instanceof Integer) {
        int pdxTypeId = (int) ent.getKey();

        try {
          pdxRegion.put(pdxTypeId, new PdxType());
        } catch (CacheWriterException cwe) {
          foundException = true;
        }
      } else {
        EnumId enumId = (EnumId) ent.getKey();

        EnumInfo enumInfo = new EnumInfo(SimpleEnum.ONE);
        try {
          pdxRegion.put(enumId, enumInfo);
        } catch (CacheWriterException cwe) {
          foundEnumException = true;
        }
      }
    }

    assertEquals(true, foundException);
    assertEquals(true, foundEnumException);
    cache.close();

  }

  @Test
  public void testPdxTypeIdWithNegativeDsId() throws Exception {
    // in this case geode will use 0 as dsId
    int dsId = -1;
    initCache(false, false, String.valueOf(dsId));
    // define a type.
    defineAType();

    Region pdxRegion = cache.getRegion(PeerTypeRegistration.REGION_NAME);
    Iterator itr = pdxRegion.entrySet().iterator();

    boolean found = false;
    boolean foundEnum = false;
    while (itr.hasNext()) {
      Map.Entry ent = (Map.Entry) itr.next();
      if (ent.getKey() instanceof Integer) {
        int pdxTypeId = (int) ent.getKey();
        PdxType pdxType = (PdxType) ent.getValue();

        int pdxTypeHashcode = pdxType.hashCode();
        System.out.println("pdx hashcode " + pdxTypeHashcode);
        int expectedPdxTypeId = PeerTypeRegistration.PLACE_HOLDER_FOR_TYPE_ID & pdxTypeHashcode;

        assertEquals(expectedPdxTypeId, pdxTypeId);

        found = true;
      } else {
        EnumId enumId = (EnumId) ent.getKey();
        EnumInfo enumInfo = (EnumInfo) ent.getValue();

        EnumInfo expectedEnumInfo = new EnumInfo(SimpleEnum.TWO);
        int expectKey =
            PeerTypeRegistration.PLACE_HOLDER_FOR_TYPE_ID & expectedEnumInfo.hashCode();;

        assertEquals(expectKey, enumId.intValue());
        foundEnum = true;
      }
    }

    assertEquals(true, found);
    cache.close();

  }

  @Test
  public void testPdxDiskStore() throws Exception {
    {
      initCache(true, true, null, "diskstore1");

      // define a type.
      defineAType();
      Region pdxRegion = cache.getRegion(PeerTypeRegistration.REGION_NAME);
      assertEquals("diskstore1", pdxRegion.getAttributes().getDiskStoreName());
      cache.close();
    }

    tearDown();
    setUp();

    {
      initCache(false, true, null);
      // define a type
      defineAType();
      Region pdxRegion = cache.getRegion(PeerTypeRegistration.REGION_NAME);
      assertEquals(DataPolicy.PERSISTENT_REPLICATE, pdxRegion.getAttributes().getDataPolicy());
      cache.close();
    }
  }

  @Test
  public void testNonPersistentRegistryWithOverflowRegion() throws Exception {
    {
      initCache(true, false, null, "diskstore1");
      cache.createRegionFactory(RegionShortcut.LOCAL_OVERFLOW).setDiskStoreName("diskstore1")
          .create("region");
      defineAType();
    }
    tearDown();
    setUp();

    {
      initCache(false, false, null);
      defineAType();
      cache.createDiskStoreFactory().setDiskDirs(new File[] {diskDir}).setMaxOplogSize(1)
          .create("diskstore1");
      cache.createRegionFactory(RegionShortcut.LOCAL_OVERFLOW).setDiskStoreName("diskstore1")
          .create("region");
    }
  }

  @Test
  public void testNonPersistentRegistryWithPersistentRegion() throws Exception {
    {
      initCache(true, false, null);
      Region region = createRegion(true, false);

      Throwable thrown = catchThrowable(() -> defineATypeNoEnum());
      assertThat(thrown).isExactlyInstanceOf(PdxInitializationException.class);

      // Drop partitioned region.
      region.destroyRegion();
      // The pdx type creation should work.
      defineATypeNoEnum();
    }
    tearDown();
    setUp();
    {
      initCache(true, false, null);
      defineATypeNoEnum();
      Throwable thrown = catchThrowable(() -> createRegion(true, false));
      assertThat(thrown).isExactlyInstanceOf(PdxInitializationException.class);
    }
  }

  @Test
  public void testNonPersistentRegistryWithPersistentPR() throws Exception {
    {
      initCache(true, false, null);
      Region region = createRegion(true, true);
      Throwable thrown = catchThrowable(() -> defineATypeNoEnum());
      assertThat(thrown).isExactlyInstanceOf(PdxInitializationException.class);

      // Drop partitioned region.
      region.destroyRegion();
      // The pdx type creation should work.
      defineATypeNoEnum();
    }
    tearDown();
    setUp();

    {
      initCache(true, false, null);
      defineATypeNoEnum();
      Throwable thrown = catchThrowable(() -> createRegion(true, true));
      assertThat(thrown).isExactlyInstanceOf(PdxInitializationException.class);
    }
  }

  @Test
  public void testPersistentRegistryWithPersistentRegion() throws Exception {
    {
      initCache(true, true, null);
      createRegion(true, false);
      defineATypeNoEnum();
    }
    tearDown();
    setUp();

    {
      initCache(true, true, null);
      defineATypeNoEnum();
      createRegion(true, false);
    }
  }

  @Test
  public void testNonPersistentRegistryWithAEQ() throws Exception {
    {
      initCache(true, false, null);
      definePersistentAEQ(cache, "aeq", true);
      Throwable thrown = catchThrowable(() -> defineATypeNoEnum());
      assertThat(thrown).isExactlyInstanceOf(PdxInitializationException.class);
    }
    tearDown();
    setUp();

    {
      initCache(true, false, null);
      defineATypeNoEnum();
      Throwable thrown = catchThrowable(() -> definePersistentAEQ(cache, "aeq", true));
      assertThat(thrown).isExactlyInstanceOf(PdxInitializationException.class);
    }
  }

  /**
   * Test that loner VMs lazily determine if they are a client or a peer.
   *
   */
  @Test
  public void testLazyLoner() throws Exception {
    // Test that we can become a peer registry
    {
      initCache(false, false, null);
      // This should work, because this is a peer.
      defineAType();
    }
    tearDown();
    setUp();

    // Test that we can become a client registry.
    {
      initCache(false, false, null);
      int port = AvailablePortHelper.getRandomAvailableTCPPort();
      PoolManager.createFactory().addServer("localhost", port).create("pool");

      Throwable thrown = catchThrowable(() -> defineAType());
      assertThat(thrown).isExactlyInstanceOf(ToDataException.class);
    }
  }

  private void defineAType() throws IOException {
    SimpleClass sc = new SimpleClass(1, (byte) 2);
    HeapDataOutputStream out = new HeapDataOutputStream(Version.CURRENT);
    DataSerializer.writeObject(sc, out);
  }

  private void defineATypeNoEnum() throws /* IO */ Exception {
    SimpleClass sc = new SimpleClass(1, (byte) 2, null);
    HeapDataOutputStream out = new HeapDataOutputStream(Version.CURRENT);
    DataSerializer.writeObject(sc, out);
  }

  private void initCache(boolean createDiskStore, boolean pdxPersist, String dsId) {
    initCache(createDiskStore, pdxPersist, dsId, null);
  }

  private void initCache(boolean createDiskStore, boolean pdxPersist, String dsId,
      String pdxDiskstore) {
    CacheFactory cf = new CacheFactory();
    cf.set(MCAST_PORT, "0");
    cf.setPdxPersistent(pdxPersist);
    if (dsId != null) {
      cf.set(ConfigurationProperties.DISTRIBUTED_SYSTEM_ID, dsId);
    }
    if (pdxDiskstore != null) {
      cf.setPdxDiskStore(pdxDiskstore);
    }
    cache = cf.create();

    if (createDiskStore) {
      cache.createDiskStoreFactory().setDiskDirs(new File[] {diskDir}).setMaxOplogSize(1)
          .create("diskstore1");
    }
  }

  private Region createRegion(boolean persistent, boolean pr) {
    Region region;
    RegionShortcut rs;

    if (persistent) {
      // cache.createDiskStoreFactory().setDiskDirs(new File[] {diskDir}).setMaxOplogSize(1)
      // .create("diskstore1");

      rs = pr ? RegionShortcut.PARTITION_PERSISTENT : RegionShortcut.LOCAL_PERSISTENT;
      region = cache.createRegionFactory(rs).setDiskStoreName("diskstore1").create("region");
    } else {
      rs = pr ? RegionShortcut.PARTITION : RegionShortcut.LOCAL;
      region = cache.createRegionFactory(rs).create("region");
    }
    return region;
  }

  private void definePersistentAEQ(Cache cache, String id, boolean persistent) {
    AsyncEventListener al = events -> true;

    cache.createAsyncEventQueueFactory().setPersistent(persistent).create(id, al);
  }

}
