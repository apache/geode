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
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;

/**
 * Tomcat specific container installation class
 *
 * Provides logic for installation of tomcat. This makes the modifications to the tomcat install as
 * described in <a href=
 * "https://geode.apache.org/docs/guide/latest/tools_modules/http_session_mgmt/session_mgmt_tomcat.html">
 * the geode docs</a>.
 */
public class TomcatInstall extends ContainerInstall {

  /**
   * Version of tomcat that this class will install
   *
   * Includes the download URL for the each version, the version number associated with each
   * version, and other properties or XML attributes needed to setup tomcat containers within Cargo
   */
  public enum TomcatVersion {
    TOMCAT6(6,
        "http://archive.apache.org/dist/tomcat/tomcat-6/v6.0.37/bin/apache-tomcat-6.0.37.zip"),
    TOMCAT7(7,
        "http://archive.apache.org/dist/tomcat/tomcat-7/v7.0.73/bin/apache-tomcat-7.0.73.zip"),
    TOMCAT8(8,
        "http://archive.apache.org/dist/tomcat/tomcat-8/v8.5.15/bin/apache-tomcat-8.5.15.zip"),
    TOMCAT9(9,
        "http://archive.apache.org/dist/tomcat/tomcat-9/v9.0.0.M21/bin/apache-tomcat-9.0.0.M21.zip");

    private final int version;
    private final String downloadURL;

    TomcatVersion(int version, String downloadURL) {
      this.version = version;
      this.downloadURL = downloadURL;
    }

    /**
     * Converts the version to an integer
     *
     * This differs from {@link #getVersion()} in that this does not always return the version
     * number. For Tomcat 9, there is no DeltaSession manager, so it must use the session manager
     * from Tomcat 8. Thus, this function returns 8 when asked for the Tomcat 9 version number.
     */
    public int toInteger() {
      switch (this) {
        case TOMCAT6:
        case TOMCAT7:
        case TOMCAT8:
          return getVersion();
        case TOMCAT9:
          return 8;
        default:
          throw new IllegalArgumentException("Illegal tomcat version option");
      }
    }

    public int getVersion() {
      return version;
    }

    public String getContainerId() {
      return "tomcat" + getVersion() + "x";
    }

    public String getDownloadURL() {
      return downloadURL;
    }

    public String jarSkipPropertyName() {
      switch (this) {
        case TOMCAT6:
          return null;
        case TOMCAT7:
          return "tomcat.util.scan.DefaultJarScanner.jarsToSkip";
        case TOMCAT8:
        case TOMCAT9:
          return "tomcat.util.scan.StandardJarScanFilter.jarsToSkip";
        default:
          throw new IllegalArgumentException("Illegal tomcat version option");
      }
    }
  }

  private static final String[] tomcatRequiredJars =
      {"antlr", "commons-lang", "fastutil", "geode-core", "geode-modules", "geode-modules-tomcat7",
          "geode-modules-tomcat8", "javax.transaction-api", "jgroups", "log4j-api", "log4j-core",
          "log4j-jul", "shiro-core", "slf4j-api", "slf4j-jdk14", "commons-validator"};

  private final TomcatVersion version;

  public TomcatInstall(TomcatVersion version) throws Exception {
    this(version, ConnectionType.PEER_TO_PEER, DEFAULT_INSTALL_DIR);
  }

  public TomcatInstall(TomcatVersion version, String installDir) throws Exception {
    this(version, ConnectionType.PEER_TO_PEER, installDir);
  }

  public TomcatInstall(TomcatVersion version, ConnectionType connType) throws Exception {
    this(version, connType, DEFAULT_INSTALL_DIR);
  }

  /**
   * Download and setup an installation tomcat using the {@link ContainerInstall} constructor and
   * some extra functions this class provides
   *
   * Specifically, this function uses {@link #copyTomcatGeodeReqFiles(String)} to install geode
   * session into Tomcat, {@link #setupDefaultSettings()} to modify the context and server XML files
   * within the installation's 'conf' folder, and {@link #updateProperties()} to set the jar
   * skipping properties needed to speedup container startup.
   */
  public TomcatInstall(TomcatVersion version, ConnectionType connType, String installDir)
      throws Exception {
    // Does download and install from URL
    super(installDir, version.getDownloadURL(), connType, "tomcat");

    this.version = version;

    // Install geode sessions into tomcat install
    copyTomcatGeodeReqFiles(GEODE_BUILD_HOME + "/lib/");
    // Set some default XML attributes in server and cache XMLs
    setupDefaultSettings();

    // Add required jars copied to jar skips so container startup is faster
    if (version.jarSkipPropertyName() != null) {
      updateProperties();
    }
  }

  /**
   * Modifies the context and server XML files in the installation's 'conf' directory so that they
   * contain the session manager class ({@link #getContextSessionManagerClass()}) and life cycle
   * listener class ({@link #getServerLifeCycleListenerClass()}) respectively
   */
  public void setupDefaultSettings() {
    HashMap<String, String> attributes = new HashMap<>();

    attributes.put("className", getContextSessionManagerClass());
    editXMLFile(getDefaultContextXMLFile().getAbsolutePath(), "Tomcat", "Manager", "Context",
        attributes);

    attributes.put("className", getServerLifeCycleListenerClass());
    editXMLFile(getDefaultServerXMLFile().getAbsolutePath(), "Tomcat", "Listener", "Server",
        attributes);
  }

  /**
   * Location of the context XML file in the installation's 'conf' directory
   */
  public File getDefaultContextXMLFile() {
    return new File(getHome() + "/conf/context.xml");
  }

  /**
   * Location of the server XML file in the installation's 'conf' directory
   */
  public File getDefaultServerXMLFile() {
    return new File(getHome() + "/conf/server.xml");
  }

  /**
   * Implements {@link ContainerInstall#getContextSessionManagerClass()}
   *
   * Gets the TomcatDeltaSessionManager class associated with this {@link #version}. Use's the
   * {@link #version}'s toInteger function to do so.
   */
  @Override
  public String getContextSessionManagerClass() {
    return "org.apache.geode.modules.session.catalina.Tomcat" + version.toInteger()
        + "DeltaSessionManager";
  }

  /**
   * Implementation of {@link ContainerInstall#generateContainer(File, String)}, which generates a
   * Tomcat specific container
   *
   * Creates a {@link TomcatContainer} instance off of this installation.
   *
   * @param containerDescriptors Additional descriptors used to identify a container
   */
  @Override
  public TomcatContainer generateContainer(File containerConfigHome, String containerDescriptors)
      throws IOException {
    return new TomcatContainer(this, containerConfigHome, containerDescriptors);
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
   * Copies jars specified by {@link #tomcatRequiredJars} from the {@link #getModulePath()} and the
   * specified other directory passed to the function
   *
   * @throws IOException if the {@link #getModulePath()}, installation lib directory, or extra
   *         directory passed in contain no files.
   */
  private void copyTomcatGeodeReqFiles(String extraJarsPath) throws IOException {
    ArrayList<File> requiredFiles = new ArrayList<>();
    // The library path for the current tomcat installation
    String tomcatLibPath = getHome() + "/lib/";

    // List of required jars and form version regexps from them
    String versionRegex = "-[0-9]+.*\\.jar";
    ArrayList<Pattern> patterns = new ArrayList<>(tomcatRequiredJars.length);
    for (String jar : tomcatRequiredJars)
      patterns.add(Pattern.compile(jar + versionRegex));

    // Don't need to copy any jars already in the tomcat install
    File tomcatLib = new File(tomcatLibPath);
    if (tomcatLib.exists()) {
      try {
        for (File file : tomcatLib.listFiles())
          patterns.removeIf(pattern -> pattern.matcher(file.getName()).find());
      } catch (NullPointerException e) {
        throw new IOException("No files found in tomcat lib directory " + tomcatLibPath);
      }
    } else {
      tomcatLib.mkdir();
    }

    // Find all the required jars in the tomcatModulePath
    try {
      for (File file : (new File(getModulePath() + "/lib/")).listFiles()) {
        for (Pattern pattern : patterns) {
          if (pattern.matcher(file.getName()).find()) {
            requiredFiles.add(file);
            patterns.remove(pattern);
            break;
          }
        }
      }
    } catch (NullPointerException e) {
      throw new IOException(
          "No files found in tomcat module directory " + getModulePath() + "/lib/");
    }

    // Find all the required jars in the extraJarsPath
    try {
      for (File file : (new File(extraJarsPath)).listFiles()) {
        for (Pattern pattern : patterns) {
          if (pattern.matcher(file.getName()).find()) {
            requiredFiles.add(file);
            patterns.remove(pattern);
            break;
          }
        }
      }
    } catch (NullPointerException e) {
      throw new IOException("No files found in extra jars directory " + extraJarsPath);
    }

    // Copy the required jars to the given tomcat lib folder
    for (File file : requiredFiles) {
      Files.copy(file.toPath(), tomcatLib.toPath().resolve(file.toPath().getFileName()),
          StandardCopyOption.REPLACE_EXISTING);
      logger.debug("Copied required jar from " + file.toPath() + " to "
          + (new File(tomcatLibPath)).toPath().resolve(file.toPath().getFileName()));
    }

    logger.info("Copied required jars into the Tomcat installation");
  }

  /**
   * Update the tomcat installation property file using {@link #editPropertyFile)}
   */
  private void updateProperties() throws Exception {
    String jarsToSkip = "";
    for (String jarName : tomcatRequiredJars)
      jarsToSkip += "," + jarName + "*.jar";

    editPropertyFile(getHome() + "/conf/catalina.properties", version.jarSkipPropertyName(),
        jarsToSkip, true);
  }
}
