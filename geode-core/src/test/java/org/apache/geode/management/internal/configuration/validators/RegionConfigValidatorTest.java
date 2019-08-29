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

  @Before
  public void before() throws Exception {
    InternalCache cache = mock(InternalCache.class);
    securityService = mock(SecurityService.class);
    when(cache.getSecurityService()).thenReturn(securityService);
    validator = new RegionConfigValidator(cache);
    config = new Region();
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
  public void invalidType() throws Exception {
    config.setName("test");
    config.setType(RegionType.UNSUPPORTED);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config)).isInstanceOf(
        IllegalArgumentException.class)
        .hasMessageContaining(
            "Region type is unsupported.");
  }

  @Test
  public void validateDefaultRedundancy() throws Exception {
    config.setName("test");
    config.setType(RegionType.PARTITION_REDUNDANT);
    // the config without the explicit redundancy is valid
    validator.validate(CacheElementOperation.CREATE, config);
  }

  @Test
  public void invalidRedundancy() throws Exception {
    config.setName("test");
    config.setType(RegionType.PARTITION_REDUNDANT);
    config.setRedundantCopies(0);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config)).isInstanceOf(
        IllegalArgumentException.class)
        .hasMessageContaining(
            "redundantCopies cannot be 0");
  }

  @Test
  public void invalidRedundancy1() throws Exception {
    config.setName("test");
    config.setRedundantCopies(0);
    config.setType(RegionType.PARTITION_REDUNDANT);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config)).isInstanceOf(
        IllegalArgumentException.class)
        .hasMessageContaining(
            "redundantCopies cannot be 0");

    config.setRedundantCopies(-1);
    config.setType(RegionType.PARTITION_REDUNDANT);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config)).isInstanceOf(
        IllegalArgumentException.class)
        .hasMessageContaining(
            "redundantCopies cannot be less than 0 or greater than 3.");

    config.setRedundantCopies(4);
    config.setType(RegionType.PARTITION_REDUNDANT);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config)).isInstanceOf(
        IllegalArgumentException.class)
        .hasMessageContaining(
            "redundantCopies cannot be less than 0 or greater than 3.");

  }

  @Test
  public void replicateWithRedundancy() throws Exception {
    config.setName("test");
    config.setType(RegionType.REPLICATE);
    config.setRedundantCopies(2);

    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config)).isInstanceOf(
        IllegalArgumentException.class)
        .hasMessageContaining(
            "redundantCopies can only be set with PARTITION regions.");
  }
}
