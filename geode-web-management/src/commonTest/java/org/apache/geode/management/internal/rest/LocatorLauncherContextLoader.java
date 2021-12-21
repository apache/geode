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

package org.apache.geode.management.internal.rest;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.junit.rules.TemporaryFolder;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.api.ClusterManagementService;

public class LocatorLauncherContextLoader extends BaseLocatorContextLoader {

  private final LocatorLauncher locator;

  private final TemporaryFolder temp;

  public LocatorLauncherContextLoader() {
    LocatorLauncher.Builder builder = new LocatorLauncher.Builder()
        .setPort(0)
        .setMemberName("locator-0")
        .set(ConfigurationProperties.LOG_LEVEL, "config");
    temp = new TemporaryFolder();
    try {
      temp.create();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    builder.setWorkingDirectory(temp.getRoot().getAbsolutePath());
    locator = builder.build();
  }

  @Override
  public void start() {
    locator.start();
  }

  @Override
  public void stop() {
    locator.stop();
    temp.delete();
  }


  @Override
  public int getPort() {
    return locator.getPort();
  }

  @Override
  public SecurityService getSecurityService() {
    return ((InternalLocator) locator.getLocator()).getCache().getSecurityService();
  }

  @Override
  public ClusterManagementService getClusterManagementService() {
    return ((InternalLocator) locator.getLocator()).getClusterManagementService();
  }
}
