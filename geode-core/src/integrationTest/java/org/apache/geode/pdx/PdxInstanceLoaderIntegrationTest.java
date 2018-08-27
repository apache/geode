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
package org.apache.geode.pdx;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.test.junit.categories.SerializationTest;

@Category(SerializationTest.class)
public class PdxInstanceLoaderIntegrationTest {

  private Cache cache;

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void loadOfPdxInstanceWithReadSerializedFalseAttemptsToDeserialize() {
    cache = new CacheFactory().set(MCAST_PORT, "0").setPdxReadSerialized(false).create();
    Region region = cache.createRegionFactory(RegionShortcut.LOCAL)
        .setCacheLoader(new PdxInstanceLoader()).create("region");
    Throwable thrown = catchThrowable(() -> region.get("key"));
    assertThat(thrown).isInstanceOf(PdxSerializationException.class);
  }

  @Test
  public void loadOfPdxInstanceWithReadSerializedTrueReturnsPdxInstance() {
    cache = new CacheFactory().set(MCAST_PORT, "0").setPdxReadSerialized(true).create();
    Region region = cache.createRegionFactory(RegionShortcut.LOCAL)
        .setCacheLoader(new PdxInstanceLoader()).create("region");
    Object obj = region.get("key");
    assertThat(obj).isInstanceOf(PdxInstance.class);
  }

  @Test
  public void loadOfPdxInstanceWithPreferCDReturnsCachedDeserializable() {
    cache = new CacheFactory().set(MCAST_PORT, "0").setPdxReadSerialized(false).create();
    LocalRegion region = (LocalRegion) cache.createRegionFactory(RegionShortcut.LOCAL)
        .setCacheLoader(new PdxInstanceLoader()).create("region");

    Object obj = region.get("key", null, true, true, true, null, null, false);

    assertThat(obj).isInstanceOf(CachedDeserializable.class);
  }

  private class PdxInstanceLoader implements CacheLoader {
    @Override
    public Object load(LoaderHelper helper) throws CacheLoaderException {
      return cache.createPdxInstanceFactory("no class name").create();
    }
  }
}
