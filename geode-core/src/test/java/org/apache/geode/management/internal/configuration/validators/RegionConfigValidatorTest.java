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

package org.apache.geode.management.internal.configuration.validators;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.RegionType;
import org.apache.geode.management.internal.CacheElementOperation;
import org.apache.geode.security.ResourcePermission;

public class RegionConfigValidatorTest {

  private RegionConfigValidator validator;
  private Region config;
  private SecurityService securityService;
  private Region.Eviction eviction;

  @Before
  public void before() throws Exception {
    InternalCache cache = mock(InternalCache.class);
    securityService = mock(SecurityService.class);
    when(cache.getSecurityService()).thenReturn(securityService);
    validator = new RegionConfigValidator(cache);
    config = new Region();
    eviction = new Region.Eviction();
  }

  @Test
  public void checkSecurityForDiskAccess() {
    config.setName("regionName");
    config.setType(RegionType.REPLICATE_PERSISTENT);
    validator.validate(CacheElementOperation.CREATE, config);

    verify(securityService).authorize(ResourcePermission.Resource.CLUSTER,
        ResourcePermission.Operation.WRITE, ResourcePermission.Target.DISK);
    assertThat(config.getType()).isEqualTo(RegionType.REPLICATE_PERSISTENT);
  }

  @Test
  public void noChangesWhenTypeIsSet() {
    config.setName("regionName");
    config.setType(RegionType.REPLICATE);
    validator.validate(CacheElementOperation.CREATE, config);

    verify(securityService, times(0)).authorize(
        any(ResourcePermission.Resource.class),
        any(ResourcePermission.Operation.class), any(ResourcePermission.Target.class));
    assertThat(config.getType()).isEqualTo(RegionType.REPLICATE);
  }

  @Test
  public void invalidName1() {
    config.setName("__test");
    config.setType(RegionType.REPLICATE);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config)).isInstanceOf(
        IllegalArgumentException.class)
        .hasMessageContaining("Region names may not begin with a double-underscore");
  }

  @Test
  public void invalidName2() {
    config.setName("a!&b");
    config.setType(RegionType.REPLICATE);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config)).isInstanceOf(
        IllegalArgumentException.class)
        .hasMessageContaining(
            "Region names may only be alphanumeric and may contain hyphens or underscores");
  }

  @Test
  public void missingType() {
    config.setName("test");
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config)).isInstanceOf(
        IllegalArgumentException.class)
        .hasMessageContaining(
            "Region type is required.");
  }

  @Test
  public void invalidType() {
    config.setName("test");
    config.setType(RegionType.LEGACY);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config)).isInstanceOf(
        IllegalArgumentException.class)
        .hasMessageContaining(
            "Region type is unsupported.");
  }



  @Test
  public void invalidRedundancy() {
    config.setRedundantCopies(-1);
    config.setType(RegionType.PARTITION);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config)).isInstanceOf(
        IllegalArgumentException.class)
        .hasMessageContaining(
            "redundantCopies cannot be less than 0 or greater than 3.");

    config.setRedundantCopies(4);
    config.setType(RegionType.PARTITION);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config)).isInstanceOf(
        IllegalArgumentException.class)
        .hasMessageContaining(
            "redundantCopies cannot be less than 0 or greater than 3.");

  }

  @Test
  public void replicateWithRedundancy() {
    config.setName("test");
    config.setType(RegionType.REPLICATE);
    config.setRedundantCopies(2);

    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config)).isInstanceOf(
        IllegalArgumentException.class)
        .hasMessageContaining(
            "redundantCopies can only be set with PARTITION regions.");
  }

  @Test
  public void validateExpiration() {
    config.setName("test");
    config.setType(RegionType.REPLICATE);
    config.addExpiry(null, null, null);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config)).isInstanceOf(
        IllegalArgumentException.class)
        .hasMessageContaining(
            "Expiration type must be set.");
    config.getExpirations().clear();

    config.addExpiry(Region.ExpirationType.ENTRY_IDLE_TIME, null, null);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config)).isInstanceOf(
        IllegalArgumentException.class)
        .hasMessageContaining(
            "Expiration timeInSeconds must be greater than or equal to 0.");
    config.getExpirations().clear();

    config.addExpiry(Region.ExpirationType.ENTRY_IDLE_TIME, -1, null);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config)).isInstanceOf(
        IllegalArgumentException.class)
        .hasMessageContaining(
            "Expiration timeInSeconds must be greater than or equal to 0.");
    config.getExpirations().clear();

    config.addExpiry(Region.ExpirationType.LEGACY, 100, null);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config)).isInstanceOf(
        IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid Expiration type.");
    config.getExpirations().clear();

    config.addExpiry(Region.ExpirationType.ENTRY_IDLE_TIME, 100,
        Region.ExpirationAction.LEGACY);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config)).isInstanceOf(
        IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid Expiration action.");
    config.getExpirations().clear();

    config.addExpiry(Region.ExpirationType.ENTRY_IDLE_TIME, 100, null);
    config.addExpiry(Region.ExpirationType.ENTRY_IDLE_TIME, 200, null);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config)).isInstanceOf(
        IllegalArgumentException.class)
        .hasMessageContaining(
            "Can not have multiple " + Region.ExpirationType.ENTRY_IDLE_TIME.name());
    config.getExpirations().clear();

    config.addExpiry(Region.ExpirationType.ENTRY_IDLE_TIME, 100, null);
    config.addExpiry(Region.ExpirationType.ENTRY_TIME_TO_LIVE, 200, null);
    validator.validate(CacheElementOperation.CREATE, config);
  }

  @Test
  public void evictionEmptyHappy() {
    config.setName("test");
    config.setType(RegionType.REPLICATE);
    config.setEviction(eviction);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Eviction type must be set.");
  }

  @Test
  public void evictionTypeNotNull() {
    config.setName("test");
    config.setType(RegionType.REPLICATE);
    eviction.setEntryCount(10);
    eviction.setObjectSizer("ObjectSizer");
    config.setEviction(eviction);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Eviction type must be set.");
  }

  @Test
  public void evictionHeapHappy() {
    config.setName("test");
    config.setType(RegionType.REPLICATE);
    eviction.setType(Region.EvictionType.HEAP_PERCENTAGE);
    config.setEviction(eviction);
    validator.validate(CacheElementOperation.CREATE, config);
  }

  @Test
  public void evictionEntryCountHappy() {
    config.setName("test");
    config.setType(RegionType.REPLICATE);
    config.setEviction(eviction);
    eviction.setEntryCount(10);
    validator.validate(CacheElementOperation.CREATE, config);
  }

  @Test
  public void evictionEntryCountMissingCount() {
    config.setName("test");
    config.setType(RegionType.REPLICATE);
    config.setEviction(eviction);
    eviction.setEntryCount(null);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("EntryCount must be set for: ENTRY_COUNT");
  }

  @Test
  public void evictionMaxMemoryMbHappy() {
    config.setName("test");
    config.setType(RegionType.REPLICATE);
    config.setEviction(eviction);
    eviction.setMemorySizeMb(10);
    validator.validate(CacheElementOperation.CREATE, config);
  }

  @Test
  public void evictionMaxMemoryMbMissingMemorySize() {
    config.setName("test");
    config.setType(RegionType.REPLICATE);
    config.setEviction(eviction);
    eviction.setMemorySizeMb(null);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("MemorySizeMb must be set for: MEMORY_SIZE");
  }

  @Test
  public void objectSizerProvidedHappy() {
    config.setName("test");
    config.setType(RegionType.REPLICATE);
    eviction.setObjectSizer("ObjectSizer");
    eviction.setMemorySizeMb(10);
    config.setEviction(eviction);
    validator.validate(CacheElementOperation.CREATE, config);

    eviction = new Region.Eviction();
    config.setEviction(eviction);
    eviction.setObjectSizer("ObjectSizer");
    eviction.setType(Region.EvictionType.HEAP_PERCENTAGE);
    validator.validate(CacheElementOperation.CREATE, config);
  }

  @Test
  public void objectSizerProvidedSad() {
    config.setName("test");
    config.setType(RegionType.REPLICATE);
    config.setEviction(eviction);
    eviction.setEntryCount(10);
    eviction.setObjectSizer("ObjectSizer");
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("ObjectSizer must not be set for: ENTRY_COUNT");
  }
}
