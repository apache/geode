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
package org.apache.geode.internal.metrics;

import static java.util.stream.Collectors.toList;
import static org.apache.geode.test.micrometer.MicrometerAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.function.BooleanSupplier;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

public class CacheMeterRegistryFactoryTest {

  private static final String SYSTEM_NAME = "member-name";
  private static final String HOST_NAME = "host-name";
  private static final boolean IS_CLIENT = false;
  private CacheMeterRegistryFactory factory;

  @Mock(answer = RETURNS_DEEP_STUBS)
  private InternalDistributedSystem system;

  @Mock
  private InternalDistributedMember member;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock
  private DistributionConfig distributionConfig;

  @Before
  public void setup() {
    when(system.getName()).thenReturn(SYSTEM_NAME);
    when(system.getConfig()).thenReturn(distributionConfig);
    when(system.getDistributedMember()).thenReturn(member);
    when(member.getHost()).thenReturn(HOST_NAME);
  }

  @Test
  public void createsCompositeMeterRegistry() {
    factory = new CacheMeterRegistryFactory();

    assertThat(factory.create(system, IS_CLIENT))
        .isInstanceOf(CompositeMeterRegistry.class);
  }

  @Test
  public void addsSystemNameAsMemberCommonTag_ifSystemNameIsNotEmpty() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();
    String theSystemName = "the-member-name";

    when(system.getName()).thenReturn(theSystemName);

    CompositeMeterRegistry registry =
        factory.create(system, IS_CLIENT);

    Meter meter = registry
        .counter("my.meter");

    assertThat(meter).hasTag("member", theSystemName);
  }

  @Test
  public void doesNotAddMemberCommonTag_ifSystemNameIsEmpty() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();
    String theMemberName = "";
    when(system.getName()).thenReturn(theMemberName);

    CompositeMeterRegistry registry =
        factory.create(system, IS_CLIENT);

    Meter meter = registry
        .counter("my.meter");

    List<String> tagNames = meter.getId().getTags().stream().map(Tag::getKey).collect(toList());
    assertThat(tagNames)
        .as("Tag names for meter with name " + meter.getId().getName())
        .doesNotContain("member");
  }

  @Test
  public void throwsNullPointerException_ifSystemNameIsNull() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();
    when(system.getName()).thenReturn(null);

    Throwable thrown =
        catchThrowable(() -> factory.create(system, IS_CLIENT));

    assertThat(thrown)
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void addsSystemIDAsClusterCommonTag_ifNotClient() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();
    int theSystemId = 21;
    when(distributionConfig.getDistributedSystemId()).thenReturn(theSystemId);

    CompositeMeterRegistry registry =
        factory.create(system, IS_CLIENT);

    Meter meter = registry
        .counter("my.meter");

    assertThat(meter.getId().getTags())
        .contains(Tag.of("cluster", String.valueOf(theSystemId)));
  }

  @Test
  public void doesNotAddClusterCommonTag_ifIsClient() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();

    CompositeMeterRegistry registry =
        factory.create(system, true);

    Meter meter = registry
        .counter("my.meter");

    List<String> tagNames = meter.getId().getTags().stream().map(Tag::getKey).collect(toList());
    assertThat(tagNames)
        .as("Tag names for meter with name " + meter.getId().getName())
        .doesNotContain("cluster");
  }

  @Test
  public void addsMemberHostNameAsHostCommonTag_ifMemberHostNameNotEmpty() {
    String memberHost = "system-host";
    factory = new CacheMeterRegistryFactory();

    when(member.getHost()).thenReturn(memberHost);

    CompositeMeterRegistry registry =
        factory.create(system, IS_CLIENT);

    Meter meter = registry
        .counter("my.meter");

    assertThat(meter)
        .hasTag("host", memberHost);
  }

  @Test
  public void throwsNullPointerException_ifMemberHostNameIsNull() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();
    when(member.getHost()).thenReturn(null);

    Throwable thrown =
        catchThrowable(() -> factory.create(system, IS_CLIENT));

    assertThat(thrown)
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void throwsIllegalArgumentException_ifMemberHostNameIsEmpty() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();
    when(member.getHost()).thenReturn("");

    Throwable thrown =
        catchThrowable(() -> factory.create(system, IS_CLIENT));

    assertThat(thrown)
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void addsLocatorMemberTypeTag_ifHasLocatorAndHasNoCacheServer() {
    BooleanSupplier hasLocator = () -> true;
    BooleanSupplier hasCacheServer = () -> false;

    factory = new CacheMeterRegistryFactory(hasLocator, hasCacheServer);

    CompositeMeterRegistry registry = factory.create(system, IS_CLIENT);

    Meter meter = registry
        .counter("my.meter");

    assertThat(meter)
        .hasTag("member.type", "locator");
  }

  @Test
  public void addsServerMemberTypeTag_ifHasCacheServerAndHasNoLocator() {
    BooleanSupplier hasLocator = () -> false;
    BooleanSupplier hasCacheServer = () -> true;

    factory = new CacheMeterRegistryFactory(hasLocator, hasCacheServer);

    CompositeMeterRegistry registry = factory.create(system, IS_CLIENT);

    Meter meter = registry
        .counter("my.meter");

    assertThat(meter)
        .hasTag("member.type", "server");
  }

  @Test
  public void addsEmbbededCacheMemberTypeTag_ifHasNoCacheServerAndHasNoLocator() {
    BooleanSupplier hasLocator = () -> false;
    BooleanSupplier hasCacheServer = () -> false;

    factory = new CacheMeterRegistryFactory(hasLocator, hasCacheServer);

    CompositeMeterRegistry registry = factory.create(system, IS_CLIENT);

    Meter meter = registry
        .counter("my.meter");

    assertThat(meter)
        .hasTag("member.type", "embedded-cache");
  }

  @Test
  public void addsServerLocatorMemberTypeTag_ifHasLocatorAndHasCacheServer() {

    BooleanSupplier hasLocator = () -> true;
    BooleanSupplier hasCacheServer = () -> true;

    factory = new CacheMeterRegistryFactory(hasLocator, hasCacheServer);

    CompositeMeterRegistry registry = factory.create(system, IS_CLIENT);

    Meter meter = registry
        .counter("my.meter");

    assertThat(meter)
        .hasTag("member.type", "server-locator");
  }
}
