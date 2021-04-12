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
import java.util.function.IntSupplier;
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
    TOMCAT6(6, "tomcat-6.0.37.zip"),
    TOMCAT7(7, "tomcat-7.0.99.zip"),
    TOMCAT8(8, "tomcat-8.5.50.zip"),
    TOMCAT9(9, "tomcat-9.0.12.zip");

    private final int version;

    private final String downloadURL;

    TomcatVersion(int version, String downloadURL) {
      this.version = version;
      this.downloadURL = downloadURL;
    }

    /**
     * Converts the version to an integer
     */
    public int toInteger() {
      return getVersion();
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

  /**
   * If you update this list method to return different dependencies, please also update the Tomcat
   * module documentation! The documentation can be found here:
   * geode-docs/tools_modules/http_session_mgmt/tomcat_installing_the_module.html.md.erb
   */
  private static final String[] tomcatRequiredJars =
      {"antlr", "commons-io", "commons-lang", "commons-validator", "fastutil", "geode-common",
          "geode-core", "geode-log4j", "geode-logging", "geode-membership", "geode-management",
          "geode-serialization",
          "geode-tcp-server", "javax.transaction-api", "jgroups", "log4j-api", "log4j-core",
          "log4j-jul", "micrometer", "shiro-core", "jetty-server", "jetty-util", "jetty-http",
          "jetty-io"};

  private final TomcatVersion version;

  public TomcatInstall(String name, TomcatVersion version, ConnectionType connectionType,
      IntSupplier portSupplier)
      throws Exception {
    this(name, version, connectionType, DEFAULT_MODULE_LOCATION, GEODE_BUILD_HOME_LIB,
        portSupplier);
  }

  /**
   * Download and setup an installation tomcat using the {@link ContainerInstall} constructor and
   * some extra functions this class provides
   *
   * Specifically, this function uses {@link #copyTomcatGeodeReqFiles(String, String)} to install
   * geode session into Tomcat, {@link #setupDefaultSettings()} to modify the context and server XML
   * files within the installation's 'conf' folder, and {@link #updateProperties()} to set the jar
   * skipping properties needed to speedup container startup.
   */
  public TomcatInstall(String name, TomcatVersion version, ConnectionType connType,
      String modulesJarLocation, String extraJarsPath, IntSupplier portSupplier)
      throws Exception {
    // Does download and install from URL
    super(name, version.getDownloadURL(), connType, "tomcat", modulesJarLocation, portSupplier);

    this.version = version;
    modulesJarLocation = getModulePath() + "/lib/";

    // Install geode sessions into tomcat install
    copyTomcatGeodeReqFiles(modulesJarLocation, extraJarsPath);
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

    // Set the session manager class within the context XML file
    attributes.put("className", getContextSessionManagerClass());
    editXMLFile(getDefaultContextXMLFile().getAbsolutePath(), "Tomcat", "Manager", "Context",
        attributes);

    // Set the server lifecycle listener within the server XML file
    attributes.put("className", getServerLifeCycleListenerClass());
    editXMLFile(getDefaultServerXMLFile().getAbsolutePath(), "Tomcat", "Listener", "Server",
        attributes);
  }

  /**
   * Get the server life cycle class that should be used
   *
   * Generates the class based on whether the installation's connection type {@link
   * ContainerInstall#connType} is client server or peer to peer.
   */
  public String getServerLifeCycleListenerClass() {
    String className = "org.apache.geode.modules.session.catalina.";
    switch (getConnectionType()) {
      case PEER_TO_PEER:
        className += "PeerToPeer";
        break;
      case CLIENT_SERVER:
      case CACHING_CLIENT_SERVER:
        className += "ClientServer";
        break;
      default:
        throw new IllegalArgumentException(
            "Bad connection type. Must be either PEER_TO_PEER or CLIENT_SERVER");
    }

    className += "CacheLifecycleListener";
    return className;
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
    return new TomcatContainer(this, containerConfigHome, containerDescriptors, portSupplier());
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
  private void copyTomcatGeodeReqFiles(String moduleJarDir, String extraJarsPath)
      throws IOException {
    ArrayList<File> requiredFiles = new ArrayList<>();
    // The library path for the current tomcat installation
    String tomcatLibPath = getHome() + "/lib/";

    // List of required jars and form version regexps from them
    String versionRegex = "-?[0-9]*.*\\.jar";
    ArrayList<Pattern> patterns = new ArrayList<>(tomcatRequiredJars.length);
    for (String jar : tomcatRequiredJars) {
      patterns.add(Pattern.compile(jar + versionRegex));
    }

    // Don't need to copy any jars already in the tomcat install
    File tomcatLib = new File(tomcatLibPath);

    // Find all jars in the tomcatModulePath and add them as required jars
    try {
      for (File file : (new File(moduleJarDir)).listFiles()) {
        if (file.isFile() && file.getName().endsWith(".jar")) {
          requiredFiles.add(file);
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
    // Adds all the required jars as jars to skip when starting Tomcat
    for (String jarName : tomcatRequiredJars) {
      jarsToSkip += "," + jarName + "*.jar";
    }

    // Add the jars to skip to the catalina property file
    editPropertyFile(getHome() + "/conf/catalina.properties", version.jarSkipPropertyName(),
        jarsToSkip, true);
  }
}
