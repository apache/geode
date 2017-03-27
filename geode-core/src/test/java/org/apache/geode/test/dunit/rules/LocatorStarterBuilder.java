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

import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_HOSTNAME_FOR_CLIENTS;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.TCP_PORT;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.security.SecurityManager;

import java.util.Properties;

public class LocatorStarterBuilder {
  private Properties properties = new Properties();
  private AvailablePort.Keeper locatorPort;
  private AvailablePort.Keeper jmxPort;
  private AvailablePort.Keeper httpPort;
  private AvailablePort.Keeper tcpPort;

  public LocatorStarterBuilder() {}

  public LocatorStarterBuilder withSecurityManager(
      Class<? extends SecurityManager> securityManager) {
    properties.setProperty(SECURITY_MANAGER, securityManager.getName());
    return this;
  }

  public LocalLocatorStarterRule buildInThisVM() {
    setDefaultProperties();
    throwIfPortsAreHardcoded();
    setPortPropertiesFromKeepers();
    return new LocalLocatorStarterRule(this);
  }

  Properties getProperties() {
    return this.properties;
  }

  private void setDefaultProperties() {
    properties.putIfAbsent(ConfigurationProperties.NAME, "locator");
    // properties.putIfAbsent(LOG_FILE, new File(properties.get(ConfigurationProperties.NAME) +
    // ".log").getAbsolutePath().toString());

    if (locatorPort == null) {
      this.locatorPort = AvailablePortHelper.getRandomAvailableTCPPortKeepers(1).get(0);
    }
    if (jmxPort == null) {
      this.jmxPort = AvailablePortHelper.getRandomAvailableTCPPortKeepers(1).get(0);
    }
    if (httpPort == null) {
      this.httpPort = AvailablePortHelper.getRandomAvailableTCPPortKeepers(1).get(0);
    }
    if (locatorPort == null) {
      this.tcpPort = AvailablePortHelper.getRandomAvailableTCPPortKeepers(1).get(0);
    }

    properties.putIfAbsent(JMX_MANAGER, "true");
    properties.putIfAbsent(JMX_MANAGER_START, "true");
    properties.putIfAbsent(HTTP_SERVICE_BIND_ADDRESS, "localhost");
    properties.putIfAbsent(JMX_MANAGER_HOSTNAME_FOR_CLIENTS, "localhost");
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

  private void throwIfPortsAreHardcoded() {
    if (properties.getProperty(JMX_MANAGER_PORT) != null
        || properties.getProperty(HTTP_SERVICE_PORT) != null
        || properties.getProperty(TCP_PORT) != null) {
      throw new IllegalArgumentException("Ports cannot be hardcoded.");
    }
  }

  AvailablePort.Keeper getLocatorPort() {
    return locatorPort;
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
}
