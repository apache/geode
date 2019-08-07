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
import java.util.Properties;
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

  private static final int CLUSTER = 42;
  private static final String MEMBER = "member-name";
  private static final String HOST = "host-name";
  private static final boolean IS_CLIENT = false;
  private static final String MEMBER_TYPE = "Server";
  @Mock(answer = RETURNS_DEEP_STUBS)
  private InternalDistributedSystem system;
  @Mock
  private InternalDistributedMember member;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  private CacheMeterRegistryFactory factory;

  @Before
  public void setup() {
    when(system.getName()).thenReturn(MEMBER);
    when(system.getDistributedMember()).thenReturn(member);
    when(member.getHost()).thenReturn(HOST);
  }

  @Test
  public void createsCompositeMeterRegistry() {
    factory = new CacheMeterRegistryFactory();

    assertThat(factory.create(CLUSTER, MEMBER, HOST, IS_CLIENT, MEMBER_TYPE))
        .isInstanceOf(CompositeMeterRegistry.class);
  }

  @Test
  public void addsMemberCommonTag_ifHasName() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();
    String theMemberName = "the-member-name";

    CompositeMeterRegistry registry =
        factory.create(CLUSTER, theMemberName, HOST, IS_CLIENT, MEMBER_TYPE);

    Meter meter = registry
        .counter("my.meter");

    assertThat(meter).hasTag("member", theMemberName);
  }

  @Test
  public void doesNotAddMemberCommonTag_ifNameIsEmpty() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();
    String theMemberName = "";

    CompositeMeterRegistry registry =
        factory.create(CLUSTER, theMemberName, HOST, IS_CLIENT, MEMBER_TYPE);

    Meter meter = registry
        .counter("my.meter");

    List<String> tagNames = meter.getId().getTags().stream().map(Tag::getKey).collect(toList());
    assertThat(tagNames)
        .as("Tag names for meter with name " + meter.getId().getName())
        .doesNotContain("member");
  }

  @Test
  public void addsClusterCommonTag_ifNotClient() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();
    int theSystemId = 21;


    CompositeMeterRegistry registry =
        factory.create(theSystemId, MEMBER, HOST, IS_CLIENT, MEMBER_TYPE);

    Meter meter = registry
        .counter("my.meter");

    assertThat(meter.getId().getTags())
        .contains(Tag.of("cluster", String.valueOf(theSystemId)));
  }

  @Test
  public void doesNotAddClusterCommonTag_ifIsClient() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();
    int theSystemId = 21;
    boolean isClient = true;

    CompositeMeterRegistry registry =
        factory.create(theSystemId, MEMBER, HOST, isClient, MEMBER_TYPE);

    Meter meter = registry
        .counter("my.meter");

    List<String> tagNames = meter.getId().getTags().stream().map(Tag::getKey).collect(toList());
    assertThat(tagNames)
        .as("Tag names for meter with name " + meter.getId().getName())
        .doesNotContain("cluster");
  }

  @Test
  public void addsHostCommonTag() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();
    String theHostName = "the-host-name";

    CompositeMeterRegistry registry =
        factory.create(CLUSTER, MEMBER, theHostName, IS_CLIENT, MEMBER_TYPE);

    Meter meter = registry
        .counter("my.meter");

    assertThat(meter.getId().getTags())
        .contains(Tag.of("host", theHostName));
  }

  @Test
  public void throwsNullPointerException_ifNameIsNull() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();
    String theMemberName = null;

    Throwable thrown =
        catchThrowable(() -> factory.create(CLUSTER, theMemberName, HOST, IS_CLIENT, MEMBER_TYPE));

    assertThat(thrown)
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void throwsNullPointerException_ifHostIsNull() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();
    String theHostName = null;

    Throwable thrown =
        catchThrowable(() -> factory.create(CLUSTER, MEMBER, theHostName, IS_CLIENT, MEMBER_TYPE));

    assertThat(thrown)
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void throwsIllegalArgumentException_ifHostIsEmpty() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();
    String theHostName = "";

    Throwable thrown =
        catchThrowable(() -> factory.create(CLUSTER, MEMBER, theHostName, IS_CLIENT, MEMBER_TYPE));

    assertThat(thrown)
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void addsSystemName_asMemberCommonTag() {
    String systemName = "the-member-name";
    factory = new CacheMeterRegistryFactory();
    when(system.getName()).thenReturn(systemName);

    CompositeMeterRegistry registry =
        factory.create(system, false);

    Meter meter = registry
        .counter("my.meter");

    assertThat(meter)
        .hasTag("member", systemName);
  }

  @Test
  public void addsSystemClusterId_asClusterCommonTag() {
    int systemId = 43;
    factory = new CacheMeterRegistryFactory();
    when(system.getConfig().getDistributedSystemId()).thenReturn(systemId);

    CompositeMeterRegistry registry =
        factory.create(system, false);

    Meter meter = registry
        .counter("my.meter");

    assertThat(meter)
        .hasTag("cluster", String.valueOf(systemId));
  }

  @Test
  public void addsSystemHost_asHostCommonTag() {
    String systemHost = "system-host";
    factory = new CacheMeterRegistryFactory();

    when(system.getDistributedMember().getHost()).thenReturn(systemHost);

    CompositeMeterRegistry registry =
        factory.create(system, false);

    Meter meter = registry
        .counter("my.meter");

    assertThat(meter)
        .hasTag("host", systemHost);
  }

  @Test
  public void addsLocatorMemberType_ifStandaloneLocator() {
    BooleanSupplier hasLocator = () -> true;
    BooleanSupplier hasCacheServer = () -> false;

    factory = new CacheMeterRegistryFactory(hasLocator, hasCacheServer);

    CompositeMeterRegistry registry = factory.create(system, false);

    Meter meter = registry
        .counter("my.meter");

    assertThat(meter)
        .hasTag("member.type", "locator");
  }

  @Test
  public void forServer_returnsServer() {
    BooleanSupplier hasLocator = () -> false;
    BooleanSupplier hasCacheServer = () -> true;

    factory = new CacheMeterRegistryFactory(hasLocator, hasCacheServer);

    CompositeMeterRegistry registry = factory.create(system, false);

    Meter meter = registry
        .counter("my.meter");

    assertThat(meter)
        .hasTag("member.type", "server");
  }

  @Test
  public void forEmbeddedCache_returnsEmbeddedCache() {
    BooleanSupplier hasLocator = () -> false;
    BooleanSupplier hasCacheServer = () -> false;

    factory = new CacheMeterRegistryFactory(hasLocator, hasCacheServer);

    CompositeMeterRegistry registry = factory.create(system, false);

    Meter meter = registry
        .counter("my.meter");

    assertThat(meter)
        .hasTag("member.type", "embedded-cache");
  }

  @Test
  public void forServerLocator_returnsServerLocator() {

    BooleanSupplier hasLocator = () -> true;
    BooleanSupplier hasCacheServer = () -> true;

    factory = new CacheMeterRegistryFactory(hasLocator, hasCacheServer);

    CompositeMeterRegistry registry = factory.create(system, false);

    Meter meter = registry
        .counter("my.meter");

    assertThat(meter)
        .hasTag("member.type", "server-locator");
  }

  @Test
  public void startLocatorRequested_returnsTrue_whenStartLocatorSetInProperties() {
    Properties properties = new Properties();
    factory = new CacheMeterRegistryFactory();
    properties.setProperty(DistributionConfig.START_LOCATOR_NAME, "127.0.0.1[10335]");
    assertThat(factory.startLocatorRequested(properties)).isTrue();
  }

  @Test
  public void startLocatorRequested_returnsFalse_whenStartLocatorPropertyDoesntExist() {
    factory = new CacheMeterRegistryFactory();
    assertThat(factory.startLocatorRequested(new Properties())).isFalse();
  }

}
