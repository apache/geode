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
package org.apache.geode.distributed.internal.locks;

import static org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID.system;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;

public class DLockServiceJUnitTest {

  DistributedSystem system;
  DistributedLockService lockService;

  @Before
  public void setup() {
    Properties properties = new Properties();
    properties.put(ConfigurationProperties.LOCATORS, "");
    properties.put(ConfigurationProperties.MCAST_PORT, "0");
    system = DistributedSystem.connect(properties);
    lockService = DistributedLockService.create("Test Lock Service", system);
  }

  @After
  public void teardown() {
    if (system != null) {
      system.disconnect();
    }
  }

  @Test
  public void locksAreReleasedDuringDisconnect() {
    assertThat(lockService.lock("MyLock", 0, -1)).isTrue();
    assertThat(lockService.isHeldByCurrentThread("MyLock")).isTrue();
    ((InternalDistributedSystem) system).setIsDisconnectThread();
    lockService.unlock("MyLock");
    assertThat(lockService.isHeldByCurrentThread("MyLock")).isFalse();
  }
}
