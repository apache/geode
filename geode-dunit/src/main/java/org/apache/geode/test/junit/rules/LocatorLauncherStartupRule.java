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

package org.apache.geode.test.junit.rules;

import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.junit.rules.MemberStarterRule.getSSLProperties;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Properties;
import java.util.function.UnaryOperator;

import org.junit.rules.TemporaryFolder;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.test.junit.rules.serializable.SerializableExternalResource;

public class LocatorLauncherStartupRule extends SerializableExternalResource {
  private LocatorLauncher launcher;
  private final TemporaryFolder temp = new TemporaryFolder();
  private final Properties properties = defaultProperties();
  private boolean autoStart;
  private UnaryOperator<LocatorLauncher.Builder> builderOperator;

  public LocatorLauncherStartupRule withAutoStart() {
    autoStart = true;
    return this;
  }

  public LocatorLauncherStartupRule withProperties(Properties properties) {
    this.properties.putAll(properties);
    return this;
  }

  public LocatorLauncherStartupRule withProperty(String key, String value) {
    this.properties.put(key, value);
    return this;
  }

  public LocatorLauncherStartupRule withSSL(String components, boolean requireAuth,
      boolean endPointIdentification) {
    Properties sslProps = getSSLProperties(components, requireAuth, endPointIdentification);
    properties.putAll(sslProps);
    return this;
  }

  public LocatorLauncherStartupRule withBuilder(
      UnaryOperator<LocatorLauncher.Builder> builderOperator) {
    this.builderOperator = builderOperator;
    return this;
  }

  @Override
  public void before() {
    if (autoStart) {
      start();
    }
  }

  public void start() {
    LocatorLauncher.Builder builder = new LocatorLauncher.Builder()
        .setPort(0)
        .set(properties)
        .set(ConfigurationProperties.LOG_LEVEL, "config");
    if (builderOperator != null) {
      builder = builderOperator.apply(builder);
    }
    if (builder.getMemberName() == null) {
      builder.setMemberName("locator-0");
    }
    try {
      temp.create();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    builder.setWorkingDirectory(temp.getRoot().getAbsolutePath());
    launcher = builder.build();
    launcher.start();
  }

  public LocatorLauncher getLauncher() {
    return launcher;
  }

  public File getWorkingDir() {
    return temp.getRoot();
  }

  @Override
  public void after() {
    if (launcher != null) {
      launcher.stop();
    }
    temp.delete();
  }

  /**
   * By default, assign available HTTP and JMX ports.
   */
  private static Properties defaultProperties() {
    Properties props = new Properties();
    int[] ports = getRandomAvailableTCPPorts(2);
    props.setProperty(HTTP_SERVICE_PORT, String.valueOf(ports[0]));
    props.setProperty(JMX_MANAGER_PORT, String.valueOf(ports[1]));
    return props;
  }
}
