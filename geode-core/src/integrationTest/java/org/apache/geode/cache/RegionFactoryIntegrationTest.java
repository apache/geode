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
package org.apache.geode.cache;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.cache.InternalRegion;

public class RegionFactoryIntegrationTest {

  private Cache cache;

  @Before
  public void setUp() {
    cache = new CacheFactory().create();
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void createRegion_LOCAL_isNotUsedForMetaRegion() {
    Region<?, ?> region = cache.createRegionFactory(RegionShortcut.LOCAL).create("region");
    InternalRegion internalRegion = (InternalRegion) region;

    boolean value = internalRegion.isUsedForMetaRegion();

    assertThat(value).isFalse();
  }

  @Test
  public void createRegion_REPLICATE_isNotUsedForMetaRegion() {
    Region<?, ?> region = cache.createRegionFactory(RegionShortcut.REPLICATE).create("region");
    InternalRegion internalRegion = (InternalRegion) region;

    boolean value = internalRegion.isUsedForMetaRegion();

    assertThat(value).isFalse();
  }

  @Test
  public void createRegion_LOCAL_isNotInternalRegion() {
    Region<?, ?> region = cache.createRegionFactory(RegionShortcut.LOCAL).create("region");
    InternalRegion internalRegion = (InternalRegion) region;

    boolean value = internalRegion.isInternalRegion();

    assertThat(value).isFalse();
  }
}
