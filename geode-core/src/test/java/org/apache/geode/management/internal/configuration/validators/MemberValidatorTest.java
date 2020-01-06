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

package org.apache.geode.management.internal.configuration.validators;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.RegionType;
import org.apache.geode.management.internal.configuration.mutators.RegionConfigManager;
import org.apache.geode.management.internal.exceptions.EntityExistsException;

public class MemberValidatorTest {

  private ConfigurationPersistenceService service;
  private RegionConfigManager regionManager;
  private Region regionConfig;
  private RegionConfig xmlRegionConfig;
  private CacheConfig cacheConfig;
  private MemberValidator validator;

  @Before
  public void before() throws Exception {
    InternalCache cache = mock(InternalCache.class);
    service = mock(ConfigurationPersistenceService.class);
    regionManager = new RegionConfigManager(null);
    when(service.getCacheConfig(any(), eq(true))).thenReturn(new CacheConfig());
    regionManager = new RegionConfigManager(null);
    validator = spy(new MemberValidator(cache, service));

    DistributedMember member1 = mock(DistributedMember.class);
    when(member1.getGroups()).thenReturn(null);
    when(member1.getName()).thenReturn("member1");

    DistributedMember member2 = mock(DistributedMember.class);
    when(member2.getGroups()).thenReturn(Collections.singletonList("group1"));

    when(member2.getName()).thenReturn("member2");

    DistributedMember member3 = mock(DistributedMember.class);
    when(member3.getGroups()).thenReturn(Collections.singletonList("group2"));

    when(member3.getName()).thenReturn("member3");

    DistributedMember member4 = mock(DistributedMember.class);
    when(member4.getGroups()).thenReturn(Arrays.asList("group1", "group2"));
    when(member4.getName()).thenReturn("member4");

    DistributedMember member5 = mock(DistributedMember.class);
    when(member5.getGroups()).thenReturn(Collections.singletonList("group3"));
    when(member5.getName()).thenReturn("member5");

    doReturn(new HashSet<>(Arrays.asList(member1, member2, member3, member4, member5)))
        .when(validator).getAllServers();

    doReturn(new HashSet<>(Arrays.asList(member1, member2, member3, member4, member5)))
        .when(validator).getAllServersAndLocators();

    when(service.getGroups())
        .thenReturn(new HashSet<>(Arrays.asList("cluster", "group1", "group2", "group3")));

    regionConfig = new Region();
    regionConfig.setName("test");
    regionConfig.setType(RegionType.REPLICATE);
    cacheConfig = new CacheConfig();

    xmlRegionConfig = new RegionConfig();
    xmlRegionConfig.setName("test");
    xmlRegionConfig.setType("REPLICATE");
  }

  @Test
  public void findServers() {
    assertThat(validator.findServers())
        .flatExtracting(DistributedMember::getName)
        .containsExactlyInAnyOrder("member1", "member2", "member3", "member4", "member5");
    assertThat(validator.findServers())
        .flatExtracting(DistributedMember::getName)
        .containsExactlyInAnyOrder("member1", "member2", "member3", "member4", "member5");

    assertThat(validator.findServers("group1")).flatExtracting(DistributedMember::getName)
        .containsExactlyInAnyOrder("member2", "member4");
    assertThat(validator.findServers("group1", "group2")).flatExtracting(DistributedMember::getName)
        .containsExactlyInAnyOrder("member2", "member3", "member4");
    assertThat(validator.findServers("group1", "group3")).flatExtracting(DistributedMember::getName)
        .containsExactlyInAnyOrder("member2", "member4", "member5");
    assertThat(validator.findServers("cluster", "group3"))
        .flatExtracting(DistributedMember::getName)
        .containsExactlyInAnyOrder("member1", "member2", "member3", "member4", "member5");
  }

  @Test
  public void findMembers() {
    assertThat(validator.findMembers(null)).flatExtracting(DistributedMember::getName)
        .containsExactlyInAnyOrder("member1", "member2", "member3", "member4", "member5");

    assertThat(validator.findMembers(null))
        .flatExtracting(DistributedMember::getName)
        .containsExactlyInAnyOrder("member1", "member2", "member3", "member4", "member5");
    assertThat(validator.findMembers(null, new String[] {null}))
        .flatExtracting(DistributedMember::getName)
        .containsExactlyInAnyOrder("member1", "member2", "member3", "member4", "member5");
    assertThat(validator.findMembers(null, "cluster"))
        .flatExtracting(DistributedMember::getName)
        .containsExactlyInAnyOrder("member1", "member2", "member3", "member4", "member5");
    assertThat(validator.findMembers(null, "Cluster"))
        .flatExtracting(DistributedMember::getName)
        .containsExactlyInAnyOrder("member1", "member2", "member3", "member4", "member5");
    assertThat(validator.findMembers(null, "CLUSTER"))
        .flatExtracting(DistributedMember::getName)
        .containsExactlyInAnyOrder("member1", "member2", "member3", "member4", "member5");
    assertThat(validator.findMembers(null, ""))
        .flatExtracting(DistributedMember::getName)
        .containsExactlyInAnyOrder("member1", "member2", "member3", "member4", "member5");

    assertThat(validator.findMembers("member1")).flatExtracting(DistributedMember::getName)
        .containsExactlyInAnyOrder("member1");

    assertThat(validator.findMembers("member1", "group1"))
        .flatExtracting(DistributedMember::getName)
        .containsExactlyInAnyOrder("member1");

    assertThat(validator.findMembers(null, "group1")).flatExtracting(DistributedMember::getName)
        .containsExactlyInAnyOrder("member2", "member4");

    assertThat(validator.findMembers(null, "group1", "cluster"))
        .flatExtracting(DistributedMember::getName)
        .containsExactlyInAnyOrder("member1", "member2", "member3", "member4", "member5");

    assertThat(validator.findMembers(null, "group1", "group2"))
        .flatExtracting(DistributedMember::getName)
        .containsExactlyInAnyOrder("member2", "member3", "member4");
  }

  @Test
  public void findGroupsWithThisElement() {
    cacheConfig.getRegions().add(xmlRegionConfig);
    when(service.getCacheConfig("cluster", true)).thenReturn(cacheConfig);
    assertThat(validator.findGroupsWithThisElement(regionConfig, regionManager))
        .containsExactly("cluster");

    when(service.getCacheConfig("cluster", true)).thenReturn(new CacheConfig());
    when(service.getCacheConfig("group1", true)).thenReturn(cacheConfig);
    when(service.getCacheConfig("group2", true)).thenReturn(cacheConfig);

    CacheConfig another = new CacheConfig();
    when(service.getCacheConfig("group3", true)).thenReturn(another);
    assertThat(validator.findGroupsWithThisElement(regionConfig, regionManager))
        .containsExactlyInAnyOrder("group1", "group2");
  }

  @Test
  public void validateCreate1() {
    cacheConfig.getRegions().add(xmlRegionConfig);
    when(service.getCacheConfig("cluster", true)).thenReturn(cacheConfig);

    regionConfig.setGroup("group1");
    assertThatThrownBy(() -> validator.validateCreate(regionConfig, regionManager))
        .isInstanceOf(EntityExistsException.class)
        .hasMessageContaining("member4").hasMessageContaining("member2")
        .hasMessageContaining("already exists on member(s)");
  }

  @Test
  public void validateCreate2() {
    cacheConfig.getRegions().add(xmlRegionConfig);
    when(service.getCacheConfig("group1", true)).thenReturn(cacheConfig);

    regionConfig.setGroup("group2");
    assertThatThrownBy(() -> validator.validateCreate(regionConfig, regionManager))
        .isInstanceOf(EntityExistsException.class)
        .hasMessageContaining("already exists on member(s) member4.");
  }

  @Test
  public void validateCreateWhenNoMemberFound() {
    cacheConfig.getRegions().add(xmlRegionConfig);
    when(service.getCacheConfig("group1", true)).thenReturn(cacheConfig);

    doReturn(Collections.emptySet()).when(validator).getAllServers();

    regionConfig.setGroup("group1");
    assertThatThrownBy(() -> validator.validateCreate(regionConfig, regionManager))
        .isInstanceOf(EntityExistsException.class)
        .hasMessageContaining("already exists in group group1");
  }

  @Test
  public void validateCreate4() {
    cacheConfig.getRegions().add(xmlRegionConfig);
    when(service.getCacheConfig("group1", true)).thenReturn(cacheConfig);

    regionConfig.setGroup("group3");
    // no exception thrown
    validator.validateCreate(regionConfig, regionManager);
  }
}
