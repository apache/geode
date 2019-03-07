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

import static org.apache.geode.internal.metrics.CompositeMeterRegistryFactory.CLUSTER_ID_TAG;
import static org.apache.geode.internal.metrics.CompositeMeterRegistryFactory.MEMBER_NAME_TAG;
import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.junit.Test;

public class CompositeMeterRegistryFactoryTest {

  private static final int CLUSTER_ID = 42;
  private static final String MEMBER_NAME = "member-name";

  @Test
  public void createsCompositeMeterRegistry() {
    CompositeMeterRegistryFactory factory = new CompositeMeterRegistryFactory() {};

    assertThat(factory.create(CLUSTER_ID, MEMBER_NAME)).isInstanceOf(CompositeMeterRegistry.class);
  }

  @Test
  public void addsMemberNameCommonTag() {
    CompositeMeterRegistryFactory factory = new CompositeMeterRegistryFactory() {};
    String theMemberName = "the-member-name";

    CompositeMeterRegistry registry = factory.create(CLUSTER_ID, theMemberName);

    Meter meter = registry.counter("my.meter");

    assertThat(meter.getId().getTags())
        .contains(Tag.of(MEMBER_NAME_TAG, theMemberName));
  }

  @Test
  public void addsClusterIdCommonTag() {
    CompositeMeterRegistryFactory factory = new CompositeMeterRegistryFactory() {};
    int theSystemId = 21;

    CompositeMeterRegistry registry = factory.create(theSystemId, MEMBER_NAME);

    Meter meter = registry.counter("my.meter");

    assertThat(meter.getId().getTags())
        .contains(Tag.of(CLUSTER_ID_TAG, String.valueOf(theSystemId)));
  }
}
