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
package org.apache.geode.distributed;

import static java.util.Arrays.asList;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.rules.DistributedRule.getLocators;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedReference;

@SuppressWarnings({"serial", "deprecation"})
public class DistributedSystemFindDistributedMembersDUnitTest implements Serializable {

  @Rule
  public DistributedReference<InternalDistributedSystem> system = new DistributedReference<>();

  @Before
  public void setUp() {
    Properties configProperties = new Properties();
    configProperties.setProperty(LOCATORS, getLocators());

    for (VM vm : asList(getVM(0), getVM(1), getVM(2))) {
      vm.invoke(() -> {
        system.set((InternalDistributedSystem) DistributedSystem.connect(configProperties));
      });
    }
  }

  @Test
  public void findDistributedMembersForLocalHostReturnsManyMembers() throws UnknownHostException {
    InetAddress localHost = LocalHostUtil.getLocalHost();

    List<VM> serverVMs = asList(getVM(0), getVM(1), getVM(2));
    for (VM vm : serverVMs) {
      vm.invoke(() -> {
        Set<DistributedMember> members = system.get().findDistributedMembers(localHost);
        // number of servers plus one locator
        assertThat(members).hasSize(serverVMs.size() + 1);
      });
    }
  }
}
