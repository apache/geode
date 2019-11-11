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
package org.apache.geode.test.dunit;

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.USE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.VALIDATE_SERIALIZABLE_OBJECTS;

import java.io.File;
import java.util.Properties;


/**
 * This class provides an abstraction over the environment that is used to run dunit. This will
 * delegate to the hydra or to the standalone dunit launcher as needed.
 *
 * <p>
 * Any dunit tests that rely on hydra configuration should go through here, so that we can separate
 * them out from depending on hydra and run them on a different VM launching system.
 */
public class DUnitEnv {

  private static DUnitEnv instance = null;

  private final String locatorHost;
  private final int locatorPort;
  private final int pid;
  private final int vmId;
  private final File workingDir;

  private DUnitEnv(String locatorHost, int locatorPort, int pid, int vmId, File workingDir) {
    this.locatorHost = locatorHost;
    this.locatorPort = locatorPort;
    this.pid = pid;
    this.vmId = vmId;
    this.workingDir = workingDir;
  }

  public static void init(String locatorHost, int locatorPort, int pid, int vmId, File workingDir) {
    instance = new DUnitEnv(locatorHost, locatorPort, pid, vmId, workingDir);
  }

  private static DUnitEnv get() {
    if (instance == null) {
      throw new Error("Distributed unit test environment is not initialized");
    }
    return instance;
  }

  public static String getLocatorString() {
    return String.format("%s[%d]", get().locatorHost, get().locatorPort);
  }

  public static int getLocatorPort() {
    return get().locatorPort;
  }

  public static Properties getDistributedSystemProperties() {
    Properties p = new Properties();
    p.setProperty(LOCATORS, getLocatorString());
    p.setProperty(MCAST_PORT, "0");
    p.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    p.setProperty(USE_CLUSTER_CONFIGURATION, "false");
    p.setProperty(VALIDATE_SERIALIZABLE_OBJECTS, "true");
    // p.setProperty(LOG_LEVEL, logLevel);
    return p;
  }

  public static int getPid() {
    return get().pid;
  }

  public static int getVMID() {
    return get().vmId;
  }

  public static File getWorkingDirectory(int pid) {
    return get().workingDir;
  }

}
