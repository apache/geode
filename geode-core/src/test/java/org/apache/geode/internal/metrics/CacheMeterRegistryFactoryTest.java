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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.List;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.junit.Test;

public class CacheMeterRegistryFactoryTest {

  private static final int CLUSTER = 42;
  private static final String MEMBER = "member-name";
  private static final String HOST = "host-name";
  private static final boolean IS_CLIENT = false;

  @Test
  public void createsCompositeMeterRegistry() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();

    assertThat(factory.create(CLUSTER, MEMBER, HOST, IS_CLIENT))
        .isInstanceOf(CompositeMeterRegistry.class);
  }

  @Test
  public void addsMemberCommonTag_ifHasName() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();
    String theMemberName = "the-member-name";

    CompositeMeterRegistry registry =
        factory.create(CLUSTER, theMemberName, HOST, IS_CLIENT);

    Meter meter = registry
        .counter("my.meter");

    assertThat(meter.getId().getTags())
        .contains(Tag.of("member", theMemberName));
  }

  @Test
  public void doesNotAddMemberCommonTag_ifNameIsEmpty() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();
    String theMemberName = "";

    CompositeMeterRegistry registry =
        factory.create(CLUSTER, theMemberName, HOST, IS_CLIENT);

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
    boolean isClient = false;

    CompositeMeterRegistry registry =
        factory.create(theSystemId, MEMBER, HOST, isClient);

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
        factory.create(theSystemId, MEMBER, HOST, isClient);

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
        factory.create(CLUSTER, MEMBER, theHostName, IS_CLIENT);

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
        catchThrowable(() -> factory.create(CLUSTER, theMemberName, HOST, IS_CLIENT));

    assertThat(thrown)
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void throwsNullPointerException_ifHostIsNull() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();
    String theHostName = null;

    Throwable thrown =
        catchThrowable(() -> factory.create(CLUSTER, MEMBER, theHostName, IS_CLIENT));

    assertThat(thrown)
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void throwsIllegalArgumentException_ifHostIsEmpty() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();
    String theHostName = "";

    Throwable thrown =
        catchThrowable(() -> factory.create(CLUSTER, MEMBER, theHostName, IS_CLIENT));

    assertThat(thrown)
        .isInstanceOf(IllegalArgumentException.class);
  }
}
