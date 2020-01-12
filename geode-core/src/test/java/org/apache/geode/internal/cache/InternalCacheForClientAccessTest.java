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
package org.apache.geode.internal.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.security.NotAuthorizedException;

public class InternalCacheForClientAccessTest {

  private InternalCache delegate;
  private InternalCacheForClientAccess cache;
  private DistributedRegion secretRegion;
  private DistributedRegion applicationRegion;
  private Set applicationRegions;
  private Set secretRegions;

  @Before
  public void setup() {
    delegate = mock(InternalCache.class);
    cache = new InternalCacheForClientAccess(delegate);
    secretRegion = mock(DistributedRegion.class);
    when(secretRegion.isInternalRegion()).thenReturn(true);
    when(secretRegion.getName()).thenReturn("secretRegion");
    applicationRegion = mock(DistributedRegion.class);
    when(applicationRegion.isInternalRegion()).thenReturn(false);
    when(applicationRegion.getName()).thenReturn("applicationRegion");
    applicationRegions = Collections.singleton(applicationRegion);
    secretRegions = Collections.singleton(secretRegion);
  }

  @Test
  public void getRegionWithApplicationWorks() {
    when(delegate.getRegion("application")).thenReturn(applicationRegion);

    Region result = cache.getRegion("application");

    assertThat(result).isSameAs(applicationRegion);
  }

  @Test
  public void getRegionWithSecretThrows() {
    when(delegate.getRegion("secret")).thenReturn(secretRegion);

    assertThatThrownBy(() -> {
      cache.getRegion("secret");
    }).isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void getFilterProfileWithApplicationWorks() {
    when(delegate.getRegion("application", true)).thenReturn(applicationRegion);
    FilterProfile filterProfile = mock(FilterProfile.class);
    when(applicationRegion.getFilterProfile()).thenReturn(filterProfile);

    FilterProfile result = cache.getFilterProfile("application");

    assertThat(result).isSameAs(filterProfile);
  }

  @Test
  public void getFilterProfileWithSecretThrows() {
    when(delegate.getRegion("secret", true)).thenReturn(secretRegion);

    assertThatThrownBy(() -> {
      cache.getFilterProfile("secret");
    }).isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void getRegionBooleanWithApplicationWorks() {
    when(delegate.getRegion("application", true)).thenReturn(applicationRegion);

    Region result = cache.getRegion("application", true);

    assertThat(result).isSameAs(applicationRegion);
  }

  @Test
  public void getRegionBooleanWithSecretThrows() {
    when(delegate.getRegion("secret", false)).thenReturn(secretRegion);

    assertThatThrownBy(() -> {
      cache.getRegion("secret", false);
    }).isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void getReinitializingRegionWithApplicationWorks() {
    when(delegate.getReinitializingRegion("application")).thenReturn(applicationRegion);

    Region result = cache.getReinitializingRegion("application");

    assertThat(result).isSameAs(applicationRegion);
  }

  @Test
  public void getReinitializingRegionWithSecretThrows() {
    when(delegate.getReinitializingRegion("secret")).thenReturn(secretRegion);

    assertThatThrownBy(() -> {
      cache.getReinitializingRegion("secret");
    }).isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void getRegionByPathWithApplicationWorks() {
    when(delegate.getInternalRegionByPath("application")).thenReturn(applicationRegion);

    Region result = cache.getInternalRegionByPath("application");

    assertThat(result).isSameAs(applicationRegion);
  }

  @Test
  public void getRegionByPathWithSecretThrows() {
    when(delegate.getInternalRegionByPath("secret")).thenReturn(secretRegion);

    assertThatThrownBy(() -> {
      cache.getInternalRegionByPath("secret");
    }).isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void getRegionByPathForProcessingWithApplicationWorks() {
    when(delegate.getRegionByPathForProcessing("application")).thenReturn(applicationRegion);

    Region result = cache.getRegionByPathForProcessing("application");

    assertThat(result).isSameAs(applicationRegion);
  }

  @Test
  public void getRegionByPathForProcessingWithSecretThrows() {
    when(delegate.getRegionByPathForProcessing("secret")).thenReturn(secretRegion);

    assertThatThrownBy(() -> {
      cache.getRegionByPathForProcessing("secret");
    }).isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void getRegionInDestroyWithApplicationWorks() {
    when(delegate.getRegionInDestroy("application")).thenReturn(applicationRegion);

    Region result = cache.getRegionInDestroy("application");

    assertThat(result).isSameAs(applicationRegion);
  }

  @Test
  public void getRegionInDestroyWithSecretThrows() {
    when(delegate.getRegionInDestroy("secret")).thenReturn(secretRegion);

    assertThatThrownBy(() -> {
      cache.getRegionInDestroy("secret");
    }).isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void getPartitionedRegionsWithApplicationWorks() {
    when(delegate.getPartitionedRegions()).thenReturn(applicationRegions);

    Set result = cache.getPartitionedRegions();

    assertThat(result).isSameAs(applicationRegions);
  }

  @Test
  public void getPartitionedRegionsWithSecretThrows() {
    when(delegate.getPartitionedRegions()).thenReturn(secretRegions);

    assertThatThrownBy(() -> {
      cache.getPartitionedRegions();
    }).isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void rootRegionsWithApplicationWorks() {
    when(delegate.rootRegions()).thenReturn(applicationRegions);

    Set result = cache.rootRegions();

    assertThat(result).isSameAs(applicationRegions);
  }

  @Test
  public void rootRegionsWithSecretThrows() {
    when(delegate.rootRegions()).thenReturn(secretRegions);

    assertThatThrownBy(() -> {
      cache.rootRegions();
    }).isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void rootRegionsWithParameterWithApplicationWorks() {
    when(delegate.rootRegions(true)).thenReturn(applicationRegions);

    Set result = cache.rootRegions(true);

    assertThat(result).isSameAs(applicationRegions);
  }

  @Test
  public void rootRegionsWithParameterWithSecretThrows() {
    when(delegate.rootRegions(true)).thenReturn(secretRegions);

    assertThatThrownBy(() -> {
      cache.rootRegions(true);
    }).isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void getAllRegionsWithApplicationWorks() {
    when(delegate.getAllRegions()).thenReturn(applicationRegions);

    Set result = cache.getAllRegions();

    assertThat(result).isSameAs(applicationRegions);
  }

  @Test
  public void getAllRegionsWithSecretThrows() {
    when(delegate.getAllRegions()).thenReturn(secretRegions);

    assertThatThrownBy(() -> {
      cache.getAllRegions();
    }).isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void createVMRegionWithNullArgsApplicationWorks() throws Exception {
    when(delegate.createVMRegion(any(), any(), any())).thenReturn(applicationRegion);

    Region result = cache.createVMRegion(null, null, null);

    assertThat(result).isSameAs(applicationRegion);
  }

  @Test
  public void createVMRegionWithDefaultArgsApplicationWorks() throws Exception {
    when(delegate.createVMRegion(any(), any(), any())).thenReturn(applicationRegion);

    Region result = cache.createVMRegion(null, null, new InternalRegionArguments());

    assertThat(result).isSameAs(applicationRegion);
  }

  @Test
  public void createVMRegionWithInternalRegionThrows() throws Exception {
    assertThatThrownBy(() -> {
      cache.createVMRegion(null, null, new InternalRegionArguments().setInternalRegion(true));
    }).isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void createVMRegionWithUsedForPartitionedRegionBucketThrows() throws Exception {
    assertThatThrownBy(() -> {
      cache.createVMRegion(null, null,
          new InternalRegionArguments().setPartitionedRegionBucketRedundancy(0));
    }).isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void createVMRegionWithUsedForMetaRegionThrows() throws Exception {
    assertThatThrownBy(() -> {
      cache.createVMRegion(null, null, new InternalRegionArguments().setIsUsedForMetaRegion(true));
    }).isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void createVMRegionWithUsedForSerialGatewaySenderQueueThrows() throws Exception {
    assertThatThrownBy(() -> {
      cache.createVMRegion(null, null,
          new InternalRegionArguments().setIsUsedForSerialGatewaySenderQueue(true));
    }).isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void createVMRegionWithUsedForParallelGatewaySenderQueueThrows() throws Exception {
    assertThatThrownBy(() -> {
      cache.createVMRegion(null, null,
          new InternalRegionArguments().setIsUsedForParallelGatewaySenderQueue(true));
    }).isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void getReconnectedCacheReturnsNull() throws Exception {
    when(delegate.getReconnectedCache()).thenReturn(null);

    Cache result = cache.getReconnectedCache();

    assertThat(result).isNull();
  }

  @Test
  public void getReconnectedCacheReturnsWrappedCache() throws Exception {
    when(delegate.getReconnectedCache()).thenReturn(delegate);

    Cache result = cache.getReconnectedCache();

    assertThat(result).isInstanceOf(InternalCacheForClientAccess.class);
  }
}
