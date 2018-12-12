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
package org.apache.geode.session.tests;

import java.io.File;
import java.io.IOException;

/**
 * Container install for a generic app server
 *
 * Extends {@link ContainerInstall} to form a basic installer which downloads and sets up an
 * installation to build a container off of. Currently being used solely for Jetty 9 installation.
 *
 * This install is used to setup many different generic app server containers using
 * {@link GenericAppServerContainer}.
 *
 * In theory, adding support for additional appserver installations should just be a matter of
 * adding new elements to the {@link GenericAppServerVersion} enumeration, since this install does
 * not do much modification of the installation itself. There is very little (maybe no) Jetty 9
 * specific code outside of the {@link GenericAppServerVersion}.
 */
public class GenericAppServerInstall extends ContainerInstall {
  private static final String JETTY_VERSION = "9.4.12.v20180830";

  /**
   * Get the version number, download URL, and container name of a generic app server using
   * hardcoded keywords
   *
   * Currently the only supported keyword instance is JETTY9.
   */
  public enum GenericAppServerVersion {
    JETTY9(9, "jetty-distribution-" + JETTY_VERSION + ".zip", "jetty");

    private final int version;
    private final String downloadURL;
    private final String containerName;

    GenericAppServerVersion(int version, String downloadURL, String containerName) {
      this.version = version;
      this.downloadURL = downloadURL;
      this.containerName = containerName;
    }

    public int getVersion() {
      return version;
    }

    public String getDownloadURL() {
      return downloadURL;
    }

    public String getContainerId() {
      return containerName + getVersion() + "x";
    }
  }

  private GenericAppServerVersion version;

  public GenericAppServerInstall(GenericAppServerVersion version)
      throws IOException, InterruptedException {
    this(version, ConnectionType.PEER_TO_PEER, DEFAULT_INSTALL_DIR);
  }

  public GenericAppServerInstall(GenericAppServerVersion version, String installDir)
      throws IOException, InterruptedException {
    this(version, ConnectionType.PEER_TO_PEER, installDir);
  }

  public GenericAppServerInstall(GenericAppServerVersion version, ConnectionType cacheType)
      throws IOException, InterruptedException {
    this(version, cacheType, DEFAULT_INSTALL_DIR);
  }

  /**
   * Download and setup container installation of a generic appserver using the
   * {@link ContainerInstall} constructor and some hardcoded module values
   */
  public GenericAppServerInstall(GenericAppServerVersion version, ConnectionType connType,
      String installDir) throws IOException, InterruptedException {
    super(installDir, version.getDownloadURL(), connType, "appserver");

    this.version = version;
  }

  /**
   * Implementation of {@link ContainerInstall#generateContainer(File, String)}, which generates a
   * generic appserver specific container
   *
   * Creates a {@link GenericAppServerContainer} instance off of this installation.
   *
   * @param containerDescriptors Additional descriptors used to identify a container
   */
  @Override
  public GenericAppServerContainer generateContainer(File containerConfigHome,
      String containerDescriptors) throws IOException {
    return new GenericAppServerContainer(this, containerConfigHome, containerDescriptors);
  }

  /**
   * The cargo specific installation id needed to setup a cargo container
   *
   * Based on the installation's {@link #version}.
   */
  @Override
  public String getInstallId() {
    return version.getContainerId();
  }

  /**
   * @see ContainerInstall#getInstallDescription()
   */
  @Override
  public String getInstallDescription() {
    return version.name() + "_" + getConnectionType().getName();
  }

  /**
   * Implements {@link ContainerInstall#getContextSessionManagerClass()}
   *
   * @throws IllegalArgumentException Always throws an illegal argument exception because app
   *         servers should not need the session manager class
   */
  @Override
  public String getContextSessionManagerClass() {
    throw new IllegalArgumentException(
        "Bad method call. Generic app servers do not use TomcatDeltaSessionManagers.");
  }
}
