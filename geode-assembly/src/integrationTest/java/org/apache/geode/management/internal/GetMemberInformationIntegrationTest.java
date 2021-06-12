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

package org.apache.geode.management.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.management.internal.functions.GetMemberInformationFunction;
import org.apache.geode.management.runtime.MemberInformation;
import org.apache.geode.test.junit.rules.LocatorLauncherStartupRule;
import org.apache.geode.test.junit.rules.ServerLauncherStartupRule;

public class GetMemberInformationIntegrationTest {
  private GetMemberInformationFunction getMemberInfoFunction;

  @Rule
  public ServerLauncherStartupRule serverWithNoDefaultServer = new ServerLauncherStartupRule()
      .withBuilder(b -> b.setDisableDefaultServer(true));

  @Rule
  public ServerLauncherStartupRule server = new ServerLauncherStartupRule();

  @Rule
  public LocatorLauncherStartupRule locator = new LocatorLauncherStartupRule();

  @Before
  public void before() {
    getMemberInfoFunction = new GetMemberInformationFunction();
  }

  @Test
  public void setServerWithNoDefaultServer() throws IOException {
    serverWithNoDefaultServer.start();
    Cache cache = serverWithNoDefaultServer.getCache();
    InternalDistributedSystem system = (InternalDistributedSystem) cache.getDistributedSystem();

    MemberInformation memberInformation = getMemberInfoFunction
        .getMemberInformation(cache, system.getConfig(), system.getDistributedMember());
    assertThat(memberInformation.isServer()).isTrue();
    assertThat(memberInformation.getCacheServerInfo()).isEmpty();
    assertThat(memberInformation.getStatus()).isEqualTo("online");
  }

  @Test
  public void regularServer() throws Exception {
    server.start();
    Cache cache = server.getCache();
    InternalDistributedSystem system = (InternalDistributedSystem) cache.getDistributedSystem();

    MemberInformation memberInformation = getMemberInfoFunction
        .getMemberInformation(cache, system.getConfig(), system.getDistributedMember());
    assertThat(memberInformation.isServer()).isTrue();
    assertThat(memberInformation.getCacheServerInfo()).hasSize(1);
    assertThat(memberInformation.getStatus()).isEqualTo("online");
  }

  @Test
  public void regularLocator() throws Exception {
    locator.start();
    Cache cache = locator.getLauncher().getCache();
    InternalDistributedSystem system = (InternalDistributedSystem) cache.getDistributedSystem();

    MemberInformation memberInformation = getMemberInfoFunction
        .getMemberInformation(cache, system.getConfig(), system.getDistributedMember());
    assertThat(memberInformation.isServer()).isFalse();
    assertThat(memberInformation.getCacheServerInfo()).isEmpty();
    assertThat(memberInformation.getStatus()).isEqualTo("online");
  }
}
