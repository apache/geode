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

import org.apache.geode.cache.configuration.EnumActionDestroyOverflow;
import org.apache.geode.cache.configuration.RegionAttributesDataPolicy;
import org.apache.geode.cache.configuration.RegionAttributesScope;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.configuration.RegionType;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.internal.CacheElementOperation;
import org.apache.geode.security.ResourcePermission;

public class RegionConfigValidatorTest {

  private RegionConfigValidator validator;
  private RegionConfig config;
  private SecurityService securityService;

  @Before
  public void before() throws Exception {
    InternalCache cache = mock(InternalCache.class);
    securityService = mock(SecurityService.class);
    when(cache.getSecurityService()).thenReturn(securityService);
    validator = new RegionConfigValidator(cache);
    config = new RegionConfig();
  }

  @Test
  public void checkSecurityForDiskAccess() {
    config.setName("regionName");
    config.setType(RegionType.REPLICATE_PERSISTENT);
    validator.validate(CacheElementOperation.CREATE, config);

    verify(securityService).authorize(ResourcePermission.Resource.CLUSTER,
        ResourcePermission.Operation.WRITE, ResourcePermission.Target.DISK);
    assertThat(config.getType()).isEqualTo("REPLICATE_PERSISTENT");
  }

  @Test
  public void noChangesWhenTypeIsSet() {
    config.setName("regionName");
    config.setType(RegionType.REPLICATE);
    validator.validate(CacheElementOperation.CREATE, config);

    verify(securityService, times(0)).authorize(
        any(ResourcePermission.Resource.class),
        any(ResourcePermission.Operation.class), any(ResourcePermission.Target.class));
    assertThat(config.getType()).isEqualTo("REPLICATE");
  }

  @Test
  public void invalidType() throws Exception {
    config.setName("regionName");
    config.setType("LOCAL");
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Type LOCAL is not supported in Management V2 API.");
  }

  @Test
  public void invalidName1() {
    config.setName("__test");
    config.setType("REPLICATE");
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config)).isInstanceOf(
        IllegalArgumentException.class)
        .hasMessageContaining("Region names may not begin with a double-underscore");
  }

  @Test
  public void invalidName2() {
    config.setName("a!&b");
    config.setType("REPLICATE");
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
            "Type of the region has to be specified");
  }

  @Test
  public void validatePartition() throws Exception {
    config.setName("test");
    config.setType("PARTITION");
    RegionAttributesType attributes = new RegionAttributesType();
    attributes.setDataPolicy(RegionAttributesDataPolicy.REPLICATE);
    config.setRegionAttributes(attributes);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void validateReplicate() throws Exception {
    config.setName("test");
    config.setType("REPLICATE");
    RegionAttributesType attributes = new RegionAttributesType();
    config.setRegionAttributes(attributes);

    attributes.setDataPolicy(RegionAttributesDataPolicy.PARTITION);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config))
        .isInstanceOf(IllegalArgumentException.class);

    attributes.setDataPolicy(RegionAttributesDataPolicy.REPLICATE);
    attributes.setScope(RegionAttributesScope.DISTRIBUTED_NO_ACK);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void validatePartition_Redundant() throws Exception {
    config.setName("test");
    config.setType("PARTITION_REDUNDANT");
    RegionAttributesType attributes = new RegionAttributesType();
    attributes.setDataPolicy(RegionAttributesDataPolicy.REPLICATE);
    config.setRegionAttributes(attributes);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void validatePartition_Persistent() throws Exception {
    config.setName("test");
    config.setType("PARTITION_PERSISTENT");
    RegionAttributesType attributes = new RegionAttributesType();
    attributes.setDataPolicy(RegionAttributesDataPolicy.REPLICATE);
    config.setRegionAttributes(attributes);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void validatePartition_Redundant_Persistent() throws Exception {
    config.setName("test");
    config.setType("PARTITION_REDUNDANT_PERSISTENT");
    RegionAttributesType attributes = new RegionAttributesType();
    attributes.setDataPolicy(RegionAttributesDataPolicy.REPLICATE);
    config.setRegionAttributes(attributes);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config))
        .isInstanceOf(IllegalArgumentException.class);
    attributes.setDataPolicy(RegionAttributesDataPolicy.PERSISTENT_PARTITION);
    attributes.setRedundantCopy("0");
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config))
        .isInstanceOf(IllegalArgumentException.class);

    // valid redundancy copy
    attributes.setRedundantCopy("2");
    validator.validate(CacheElementOperation.CREATE, config);
    assertThat(config.getRegionAttributes().getPartitionAttributes().getRedundantCopies())
        .isEqualTo("2");
  }

  @Test
  public void validatePartition_overflow() throws Exception {
    config.setName("test");
    config.setType("PARTITION_OVERFLOW");
    RegionAttributesType attributes = new RegionAttributesType();
    attributes.setDataPolicy(RegionAttributesDataPolicy.REPLICATE);
    config.setRegionAttributes(attributes);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config))
        .isInstanceOf(IllegalArgumentException.class);

    attributes.setDataPolicy(RegionAttributesDataPolicy.PARTITION);
    attributes.setLruHeapPercentageEvictionAction(EnumActionDestroyOverflow.LOCAL_DESTROY);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void validatePartition_proxy() throws Exception {
    config.setName("test");
    config.setType("PARTITION_PROXY");
    RegionAttributesType attributes = new RegionAttributesType();
    attributes.setDataPolicy(RegionAttributesDataPolicy.REPLICATE);
    config.setRegionAttributes(attributes);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config))
        .isInstanceOf(IllegalArgumentException.class);

    attributes.setDataPolicy(RegionAttributesDataPolicy.PARTITION);
    attributes.setLocalMaxMemory("5000");
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config))
        .isInstanceOf(IllegalArgumentException.class);

    // validator will use the type to set the local max memory to be 0
    attributes.setLocalMaxMemory(null);
    validator.validate(CacheElementOperation.CREATE, config);
    assertThat(attributes.getPartitionAttributes().getLocalMaxMemory()).isEqualTo("0");
  }
}
