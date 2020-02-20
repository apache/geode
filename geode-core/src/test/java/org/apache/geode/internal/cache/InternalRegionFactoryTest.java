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

package org.apache.geode.internal.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Region;

public class InternalRegionFactoryTest {
  private InternalCache cache;

  @Before
  public void init() {
    cache = mock(InternalCache.class);
  }

  @Test
  public void createWithInternalRegionArgumentsCallsCreateVMRegion() throws Exception {
    InternalRegionFactory<Object, Object> regionFactory = new InternalRegionFactory<>(cache);
    regionFactory.setInternalRegion(true);
    InternalRegionArguments internalRegionArguments = regionFactory.getInternalRegionArguments();
    String regionName = "regionName";

    regionFactory.create(regionName);

    assertThat(internalRegionArguments).isNotNull();
    verify(cache).createVMRegion(same(regionName), any(), same(internalRegionArguments));
  }

  @Test
  public void createWithInternalRegionArgumentsReturnsRegion() throws Exception {
    InternalRegionFactory<Object, Object> regionFactory = new InternalRegionFactory<>(cache);
    regionFactory.setInternalRegion(true);
    InternalRegionArguments internalRegionArguments = regionFactory.getInternalRegionArguments();
    String regionName = "regionName";
    Region<Object, Object> expectedRegion = mock(Region.class);
    when(cache.createVMRegion(same(regionName), any(), same(internalRegionArguments)))
        .thenReturn(expectedRegion);

    Region<Object, Object> region = regionFactory.create(regionName);

    assertThat(region).isSameAs(expectedRegion);
  }

  @Test
  public void createWithoutInternalRegionArgumentsCallsCreateRegion() throws Exception {
    InternalRegionFactory<Object, Object> regionFactory = new InternalRegionFactory<>(cache);
    String regionName = "regionName";

    regionFactory.create(regionName);

    verify(cache).createRegion(same(regionName), any());
  }

  @Test
  public void createSubregionWithInternalRegionArgumentsReturnsRegion() throws Exception {
    InternalRegionFactory<Object, Object> regionFactory = new InternalRegionFactory<>(cache);
    regionFactory.setInternalRegion(true);
    InternalRegionArguments internalRegionArguments = regionFactory.getInternalRegionArguments();
    String regionName = "regionName";
    Region<Object, Object> expectedRegion = mock(Region.class);
    InternalRegion parent = mock(InternalRegion.class);
    when(parent.createSubregion(same(regionName), any(), same(internalRegionArguments)))
        .thenReturn(expectedRegion);

    Region<Object, Object> region = regionFactory.createSubregion(parent, regionName);

    assertThat(region).isSameAs(expectedRegion);
  }

  @Test
  public void createSubregionWithoutInternalRegionArgumentsCallsParentCreateSubregion()
      throws Exception {
    InternalRegionFactory<Object, Object> regionFactory = new InternalRegionFactory<>(cache);
    String regionName = "regionName";
    Region<?, ?> parent = mock(InternalRegion.class);

    regionFactory.createSubregion(parent, regionName);

    verify(parent).createSubregion(same(regionName), any());
  }
}
