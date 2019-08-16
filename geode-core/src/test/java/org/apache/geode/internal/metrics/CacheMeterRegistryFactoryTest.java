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

import static org.apache.geode.internal.lang.SystemUtils.isWindows;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.Collection;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Test;

public class CacheMeterRegistryFactoryTest {

  private static final int CLUSTER_ID = 42;
  private static final String MEMBER_NAME = "member-name";
  private static final String HOST_NAME = "host-name";

  @Test
  public void createsCompositeMeterRegistry() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();

    assertThat(factory.create(CLUSTER_ID, MEMBER_NAME, HOST_NAME))
        .isInstanceOf(CompositeMeterRegistry.class);
  }

  @Test
  public void addsMemberNameCommonTag() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();
    String theMemberName = "the-member-name";

    CompositeMeterRegistry registry = factory.create(CLUSTER_ID, theMemberName, HOST_NAME);

    Meter meter = registry
        .counter("my.meter");

    assertThat(meter.getId().getTags())
        .contains(Tag.of("member.name", theMemberName));
  }

  @Test
  public void addsClusterIdCommonTag() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();
    int theSystemId = 21;

    CompositeMeterRegistry registry = factory.create(theSystemId, MEMBER_NAME, HOST_NAME);

    Meter meter = registry
        .counter("my.meter");

    assertThat(meter.getId().getTags())
        .contains(Tag.of("cluster.id", String.valueOf(theSystemId)));
  }

  @Test
  public void addsHostNameCommonTag() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();
    String theHostName = "the-host-name";

    CompositeMeterRegistry registry = factory.create(CLUSTER_ID, MEMBER_NAME, theHostName);

    Meter meter = registry
        .counter("my.meter");

    assertThat(meter.getId().getTags())
        .contains(Tag.of("host.name", theHostName));
  }

  @Test
  public void addsGaugesForHeapMemory() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();

    CompositeMeterRegistry registry = factory.create(CLUSTER_ID, MEMBER_NAME, HOST_NAME);
    registry.add(new SimpleMeterRegistry());

    Collection<Gauge> heapGauges = registry
        .find("jvm.memory.used")
        .tag("area", "heap")
        .gauges();

    assertThat(heapGauges).isNotEmpty();
  }

  @Test
  public void addsGaugesForNonHeapUsedMemory() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();

    CompositeMeterRegistry registry = factory.create(CLUSTER_ID, MEMBER_NAME, HOST_NAME);
    registry.add(new SimpleMeterRegistry());

    Collection<Gauge> nonheapGauges = registry
        .find("jvm.memory.used")
        .tag("area", "nonheap")
        .gauges();

    assertThat(nonheapGauges).isNotEmpty();
  }

  @Test
  public void addsMetersForJvmThreadMetricsBinder() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();

    CompositeMeterRegistry registry = factory.create(CLUSTER_ID, MEMBER_NAME, HOST_NAME);
    registry.add(new SimpleMeterRegistry());

    Collection<Meter> meters = registry
        .find("jvm.threads.peak")
        .meters();

    assertThat(meters).isNotEmpty();
  }

  @Test
  public void addsMetersForJvmGcMetricsBinder() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();

    CompositeMeterRegistry registry = factory.create(CLUSTER_ID, MEMBER_NAME, HOST_NAME);
    registry.add(new SimpleMeterRegistry());

    Collection<Meter> meters = registry
        .find("jvm.gc.max.data.size")
        .meters();

    assertThat(meters).isNotEmpty();
  }

  @Test
  public void addsMetersForProcessorMetricsBinder() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();

    CompositeMeterRegistry registry = factory.create(CLUSTER_ID, MEMBER_NAME, HOST_NAME);
    registry.add(new SimpleMeterRegistry());

    Collection<Meter> meters = registry
        .find("system.cpu.count")
        .meters();

    assertThat(meters).isNotEmpty();
  }

  @Test
  public void addsMetersForUptimeMetricsBinder() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();

    CompositeMeterRegistry registry = factory.create(CLUSTER_ID, MEMBER_NAME, HOST_NAME);
    registry.add(new SimpleMeterRegistry());

    Collection<Meter> meters = registry
        .find("process.uptime")
        .meters();

    assertThat(meters).isNotEmpty();
  }

  @Test
  public void addsMetersForFileDescriptorMetricsBinder() {
    assumeThat(isWindows()).isFalse();

    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();

    CompositeMeterRegistry registry = factory.create(CLUSTER_ID, MEMBER_NAME, HOST_NAME);
    registry.add(new SimpleMeterRegistry());

    Collection<Meter> meters = registry
        .find("process.files.open")
        .meters();

    assertThat(meters).isNotEmpty();
  }
}
