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
package org.apache.geode.test.dunit.examples;

import static java.util.Arrays.asList;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMId;
import static org.apache.geode.test.dunit.rules.DistributedRule.getLocators;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedReference;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

@SuppressWarnings("serial")
public class DistributedReferenceServerLauncherExampleTest implements Serializable {

  @Rule
  public DistributedReference<ServerLauncher> serverLauncher = new DistributedReference<>();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Before
  public void setUp() {
    String locators = getLocators();

    for (VM vm : asList(getVM(0), getVM(1), getVM(2), getVM(3), getController())) {
      vm.invoke(() -> {
        String name = "server-" + getVMId();
        ServerLauncher serverLauncher = new ServerLauncher.Builder()
            .setWorkingDirectory(temporaryFolder.newFolder(name).getAbsolutePath())
            .setMemberName(name)
            .setDisableDefaultServer(true)
            .set(LOCATORS, locators)
            .set(HTTP_SERVICE_PORT, "0")
            .set(JMX_MANAGER_PORT, "0")
            .build();
        serverLauncher.start();
        this.serverLauncher.set(serverLauncher);
      });
    }
  }

  @Test
  public void eachVmHasItsOwnLocatorLauncher() {
    for (VM vm : asList(getVM(0), getVM(1), getVM(2), getVM(3), getController())) {
      vm.invoke(() -> {
        assertThat(serverLauncher.get()).isNotNull();

        InternalCache cache = (InternalCache) serverLauncher.get().getCache();
        InternalDistributedSystem system = cache.getInternalDistributedSystem();
        assertThat(system.getAllOtherMembers()).hasSize(5);
      });
    }
  }
}
