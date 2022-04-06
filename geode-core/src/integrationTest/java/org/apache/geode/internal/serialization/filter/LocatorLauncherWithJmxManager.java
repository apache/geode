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
package org.apache.geode.internal.serialization.filter;

import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;

import java.nio.file.Path;

import org.junit.Before;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.junit.rules.CloseableReference;

abstract class LocatorLauncherWithJmxManager {

  static final String NAME = "locator";
  static final String JDK_PROPERTY = "jdk.serialFilter";
  static final String JMX_PROPERTY = "jmx.remote.rmi.server.serial.filter.pattern";

  static final ObjectInputFilterApi OBJECT_INPUT_FILTER_API =
      new ReflectiveObjectInputFilterApiFactory().createObjectInputFilterApi();

  Path workingDirectory;
  int locatorPort;
  int jmxPort;
  Path logFile;
  String openMBeanFilterPattern;

  @Rule
  public CloseableReference<LocatorLauncher> locator = new CloseableReference<>();
  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUpFiles() {
    workingDirectory = temporaryFolder.getRoot().toPath().toAbsolutePath();
    logFile = workingDirectory.resolve(NAME + ".log").toAbsolutePath();
  }

  @Before
  public void setUpPorts() {
    int[] ports = getRandomAvailableTCPPorts(2);
    jmxPort = ports[0];
    locatorPort = ports[1];
  }

  @Before
  public void setUpFilterPattern() {
    openMBeanFilterPattern = new OpenMBeanFilterPattern().pattern();
  }

  SystemManagementService getSystemManagementService() {
    InternalLocator locator = (InternalLocator) this.locator.get().getLocator();
    InternalCache cache = locator.getCache();
    return (SystemManagementService) ManagementService.getManagementService(cache);
  }

  boolean isJmxManagerStarted() {
    return getSystemManagementService().isManager();
  }
}
