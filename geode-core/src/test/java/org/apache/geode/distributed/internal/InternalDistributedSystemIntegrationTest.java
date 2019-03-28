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

import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.internal.InternalDistributedSystem.ALLOW_MULTIPLE_SYSTEMS_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import org.apache.geode.distributed.DistributedSystem;

public class InternalDistributedSystemIntegrationTest {

  private InternalDistributedSystem system;
  private InternalDistributedSystem system2;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @After
  public void tearDown() {
    if (system != null) {
      system.disconnect();
    }
    if (system2 != null) {
      system2.disconnect();
    }
  }

  @Test
  public void connect() {
    String theName = "theName";
    Properties configProperties = new Properties();
    configProperties.setProperty(NAME, theName);

    system = (InternalDistributedSystem) DistributedSystem.connect(configProperties);

    assertThat(system.isConnected()).isTrue();
    assertThat(system.getName()).isEqualTo(theName);
  }

  @Test
  public void connectWithAllowsMultipleSystems() {
    System.setProperty(ALLOW_MULTIPLE_SYSTEMS_PROPERTY, "true");

    String name1 = "name1";
    Properties configProperties1 = new Properties();
    configProperties1.setProperty(NAME, name1);

    system = (InternalDistributedSystem) DistributedSystem.connect(configProperties1);

    String name2 = "name2";
    Properties configProperties2 = new Properties();
    configProperties2.setProperty(NAME, name2);

    system2 = (InternalDistributedSystem) DistributedSystem.connect(configProperties2);

    assertThat(system.isConnected()).isTrue();
    assertThat(system.getName()).isEqualTo(name1);

    assertThat(system2.isConnected()).isTrue();
    assertThat(system2.getName()).isEqualTo(name2);

    assertThat(system2).isNotSameAs(system);
  }
}
