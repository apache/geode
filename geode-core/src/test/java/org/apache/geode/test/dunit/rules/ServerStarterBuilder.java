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

package org.apache.geode.test.dunit.rules;

import static org.apache.geode.distributed.ConfigurationProperties.DEPLOY_WORKING_DIR;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.START_DEV_REST_API;
import static org.apache.geode.distributed.ConfigurationProperties.TCP_PORT;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.security.SecurityManager;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ServerStarterBuilder {
  private Properties properties = new Properties();
  private Map<String, RegionShortcut> regionsToCreate = new HashMap<>();

  private AvailablePort.Keeper serverPort;
  private AvailablePort.Keeper jmxPort;
  private AvailablePort.Keeper httpPort;
  private AvailablePort.Keeper tcpPort;

  /**
   * If this flag is true, the Rule will create a temporary folder and set the server's
   * DEPLOY_WORKING_DIR to that folder. Otherwise, a user must manage their own working directory.
   */
  private boolean hasAutomaticallyManagedWorkingDir;

  public LocalServerStarterRule buildInThisVM() {
    setDefaultProperties();
    throwIfPortsAreHardcoded();
    setPortPropertiesFromKeepers();

    return new LocalServerStarterRule(this);
  }

  public ServerStarterBuilder withJMXManager() {
    if (this.jmxPort == null) {
      this.jmxPort = AvailablePortHelper.getRandomAvailableTCPPortKeepers(1).get(0);
    }

    if (this.httpPort == null) {
      this.httpPort = AvailablePortHelper.getRandomAvailableTCPPortKeepers(1).get(0);
    }

    properties.setProperty(JMX_MANAGER, "true");
    properties.setProperty(JMX_MANAGER_START, "true");
    properties.setProperty(HTTP_SERVICE_BIND_ADDRESS, "localhost");
    return this;
  }

  /**
   * Enables the Dev REST API with a random http port
   */
  public ServerStarterBuilder withRestService() {
    if (this.httpPort == null) {
      this.httpPort = AvailablePortHelper.getRandomAvailableTCPPortKeepers(1).get(0);
    }

    properties.setProperty(START_DEV_REST_API, "true");
    properties.setProperty(HTTP_SERVICE_BIND_ADDRESS, "localhost");
    return this;
  }

  public ServerStarterBuilder withSecurityManager(
      Class<? extends SecurityManager> securityManager) {
    properties.setProperty(SECURITY_MANAGER, securityManager.getName());
    return this;
  }

  public ServerStarterBuilder withProperty(String key, String value) {
    properties.setProperty(key, value);
    return this;
  }

  public ServerStarterBuilder withRegion(RegionShortcut type, String name) {
    regionsToCreate.put(name, type);
    return this;
  }


  private void setDefaultProperties() {
    String workingDir = properties.getProperty(DEPLOY_WORKING_DIR);
    if (workingDir == null) {
      hasAutomaticallyManagedWorkingDir = true;
    }

    properties.putIfAbsent(ConfigurationProperties.NAME, "server");

    if (this.serverPort == null) {
      this.serverPort = AvailablePortHelper.getRandomAvailableTCPPortKeepers(1).get(0);
    }

    if (this.tcpPort == null) {
      this.tcpPort = AvailablePortHelper.getRandomAvailableTCPPortKeepers(1).get(0);
    }

    properties.putIfAbsent(MCAST_PORT, "0");
    properties.putIfAbsent(LOCATORS, "");
  }

  /**
   * We want to make sure that all tests use AvailablePort.Keeper rather than setting port numbers
   * manually so that we can avoid flaky tests caused by BindExceptions.
   * 
   * @throws IllegalArgumentException - if ports are hardcoded.
   */
  private void throwIfPortsAreHardcoded() {
    if (properties.getProperty(JMX_MANAGER_PORT) != null
        || properties.getProperty(HTTP_SERVICE_PORT) != null
        || properties.getProperty(TCP_PORT) != null) {
      throw new IllegalArgumentException("Ports cannot be hardcoded.");
    }
  }

  private void setPortPropertiesFromKeepers() {
    if (jmxPort != null) {
      properties.setProperty(JMX_MANAGER_PORT, Integer.toString(jmxPort.getPort()));
    }

    if (httpPort != null) {
      properties.setProperty(HTTP_SERVICE_PORT, Integer.toString(httpPort.getPort()));
    }

    if (tcpPort != null) {
      properties.setProperty(TCP_PORT, Integer.toString(tcpPort.getPort()));

    }
  }

  AvailablePort.Keeper getServerPort() {
    return serverPort;
  }

  AvailablePort.Keeper getJmxPort() {
    return jmxPort;
  }

  AvailablePort.Keeper getHttpPort() {
    return httpPort;
  }

  AvailablePort.Keeper getTcpPort() {
    return tcpPort;
  }

  Map<String, RegionShortcut> getRegionsToCreate() {
    return this.regionsToCreate;
  }

  Properties getProperties() {
    return this.properties;
  }

  boolean hasAutomaticallyManagedWorkingDir() {
    return this.hasAutomaticallyManagedWorkingDir;
  }

}
