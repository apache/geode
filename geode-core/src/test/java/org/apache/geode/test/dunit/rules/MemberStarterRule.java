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
 *
 */

package org.apache.geode.test.dunit.rules;

import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;

import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.security.SecurityManager;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.security.SecurityManager;

/**
 * the abstract class that's used by LocatorStarterRule and ServerStarterRule to avoid code
 * duplication.
 */
public abstract class MemberStarterRule<T> extends ExternalResource implements Member {
  protected transient TemporaryFolder temporaryFolder;
  protected String oldUserDir;

  protected File workingDir;
  protected int memberPort = -1;
  protected int jmxPort = -1;
  protected int httpPort = -1;

  protected String name;
  protected Properties properties = new Properties();

  protected boolean autoStart = false;

  public MemberStarterRule() {
    this(null);
  }

  public MemberStarterRule(File workDir) {
    workingDir = workDir;
    oldUserDir = System.getProperty("user.dir");
    if (workingDir == null) {
      temporaryFolder = new TemporaryFolder();
      try {
        temporaryFolder.create();
      } catch (IOException e) {
        throw new RuntimeException(e.getMessage(), e);
      }
      workingDir = temporaryFolder.getRoot().getAbsoluteFile();
    }

    System.setProperty("user.dir", workingDir.toString());
    // initial values
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");
  }

  @Override
  public void after() {
    // invoke stopMember() first and then ds.disconnect
    stopMember();

    DistributedSystem ds = InternalDistributedSystem.getConnectedInstance();
    if (ds != null) {
      ds.disconnect();
    }

    if (oldUserDir == null) {
      System.clearProperty("user.dir");
    } else {
      System.setProperty("user.dir", oldUserDir);
    }

    if (temporaryFolder != null) {
      temporaryFolder.delete();
    }
  }

  public T withProperty(String key, String value) {
    properties.setProperty(key, value);
    return (T) this;
  }

  public T withProperties(Properties props) {
    if (props != null) {
      this.properties.putAll(props);
    }
    return (T) this;
  }

  public T withSecurityManager(Class<? extends SecurityManager> securityManager) {
    properties.setProperty(SECURITY_MANAGER, securityManager.getName());
    return (T) this;
  }

  public T withAutoStart() {
    this.autoStart = true;
    return (T) this;
  }

  public T withName(String name) {
    this.name = name;
    properties.setProperty(NAME, name);
    // if log-file is not already set
    properties.putIfAbsent(LOG_FILE, new File(name + ".log").getAbsolutePath());
    return (T) this;
  }

  public T withConnectionToLocator(int locatorPort) {
    if (locatorPort > 0) {
      properties.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
    }
    return (T) this;
  }

  /**
   * be able to start JMX manager and admin rest on default ports
   */
  public T withJMXManager(boolean useProductDefaultPorts) {
    if (!useProductDefaultPorts) {
      // do no override these properties if already exists
      properties.putIfAbsent(JMX_MANAGER_PORT,
          AvailablePortHelper.getRandomAvailableTCPPort() + "");
      properties.putIfAbsent(HTTP_SERVICE_PORT,
          AvailablePortHelper.getRandomAvailableTCPPort() + "");
      this.jmxPort = Integer.parseInt(properties.getProperty(JMX_MANAGER_PORT));
      this.httpPort = Integer.parseInt(properties.getProperty(HTTP_SERVICE_PORT));
    } else {
      // the real port numbers will be set after we started the server/locator.
      this.jmxPort = 0;
      this.httpPort = 0;
    }
    properties.putIfAbsent(JMX_MANAGER, "true");
    properties.putIfAbsent(JMX_MANAGER_START, "true");
    properties.putIfAbsent(HTTP_SERVICE_BIND_ADDRESS, "localhost");
    return (T) this;
  }

  /**
   * start the jmx manager and admin rest on a random ports
   */
  public T withJMXManager() {
    return withJMXManager(false);
  }

  protected void normalizeProperties() {
    // if name is set via property, not with API
    if (name == null) {
      if (properties.containsKey(NAME)) {
        name = properties.getProperty(NAME);
      } else {
        if (this instanceof ServerStarterRule)
          name = "server";
        else {
          name = "locator";
        }
      }
      withName(name);
    }

    // if jmxPort is set via property, not with API
    if (jmxPort < 0 && properties.containsKey(JMX_MANAGER_PORT)) {
      // this will make sure we have all the missing properties, but it won't override
      // the existing properties
      withJMXManager(false);
    }
  }

  abstract void stopMember();

  @Override
  public File getWorkingDir() {
    return workingDir;
  }

  @Override
  public int getPort() {
    return memberPort;
  }

  @Override
  public int getJmxPort() {
    return jmxPort;
  }

  @Override
  public int getHttpPort() {
    return httpPort;
  }

  @Override
  public String getName() {
    return name;
  }
}
