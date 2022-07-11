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
package org.apache.geode.distributed.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.logging.InternalLogWriter;

public class LonerDistributionManagerTest {

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Test
  public void getEquivalentsForLocalHostReturnsOneAddress() throws UnknownHostException {
    DistributionConfig distributionConfig = mock(DistributionConfig.class);
    InetAddress localHost = LocalHostUtil.getLocalHost();
    InternalDistributedSystem system = mock(InternalDistributedSystem.class);
    InternalLogWriter logWriter = mock(InternalLogWriter.class);

    when(system.getConfig()).thenReturn(distributionConfig);

    DistributionManager distributionManager = new LonerDistributionManager(system, logWriter);

    Set<InetAddress> members = distributionManager.getEquivalents(localHost);

    assertThat(members).contains(localHost);
  }
}
