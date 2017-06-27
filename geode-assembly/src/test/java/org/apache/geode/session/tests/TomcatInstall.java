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

import org.codehaus.cargo.container.configuration.FileConfig;
import org.codehaus.cargo.container.configuration.LocalConfiguration;

/**
 * Tomcat specific container installation class
 *
 * Provides logic for installation and setup of the tomcat container, including editing the
 * properties needed to switch between cache types.
 *
 * This makes the modifications to the tomcat install as described in <a href=
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

    public String downloadURL() {
      return downloadURL;
    }

    public int toInteger() {
      return version;
    }

    /**
     * Name of the property that can be set to speed up container startup
     *
     * Tomcat versions have different property names for the property that, when set, causes the
     * container to skip over the specified jars when scanning jars on startup. Similar to
     * {@link GenericAppServerInstall.Server} but specifically built for Tomcat installations.
     * 
     * @throws IllegalArgumentException if given a tomcat version is not expected
     */
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

    /**
     * XML required DeltaSessionManager attribute
     *
     * This XML attribute changes based on the Tomcat version.
     * 
     * @return HashMap whose key is the name of the attribute ('className') and value is the value
     *         of the attribute.
     */
    public HashMap<String, String> getRequiredXMLAttributes() {
      HashMap<String, String> attributes = new HashMap<>();

      int sessionManagerNum;
      switch (this) {
        case TOMCAT9:
          sessionManagerNum = 8;
          break;
        default:
          sessionManagerNum = this.toInteger();
      }
      attributes.put("className", "org.apache.geode.modules.session.catalina.Tomcat"
          + sessionManagerNum + "DeltaSessionManager");
      return attributes;
    }
  }

  /**
   * Tomcat cache type configuration for this installation
   *
   * Contains the XML needed for each cache type. Similar to
   * {@link GenericAppServerInstall.CacheType} but specifically built for Tomcat installations.
   */
  public enum TomcatConfig {
    PEER_TO_PEER("org.apache.geode.modules.session.catalina.PeerToPeerCacheLifecycleListener",
        "cache-peer.xml"),
    CLIENT_SERVER("org.apache.geode.modules.session.catalina.ClientServerCacheLifecycleListener",
        "cache-client.xml");

    private final String XMLClassName;
    private final String XMLFile;

    TomcatConfig(String XMLClassName, String XMLFile) {
      this.XMLClassName = XMLClassName;
      this.XMLFile = XMLFile;
    }

    /**
     * Name of XML file associated with this type of cache
     */
    public String getXMLFile() {
      return XMLFile;
    }

    /**
     * Required XML attribute associated with this type of cache
     * 
     * @return HashMap whose key is the name of the attribute ('className') and value is the value
     *         of the attribute.
     */
    public HashMap<String, String> getRequiredXMLAttributes() throws IOException {
      HashMap<String, String> attributes = new HashMap<>();
      attributes.put("className", XMLClassName);
      return attributes;
    }
  }

  private static final String[] tomcatRequiredJars =
      {"antlr", "commons-lang", "fastutil", "geode-core", "geode-modules", "geode-modules-tomcat7",
          "geode-modules-tomcat8", "javax.transaction-api", "jgroups", "log4j-api", "log4j-core",
          "log4j-jul", "shiro-core", "slf4j-api", "slf4j-jdk14"};

  private TomcatConfig config;
  private final TomcatVersion version;
  private final String tomcatModulePath;

  public TomcatInstall(TomcatVersion version) throws Exception {
    this(version, TomcatConfig.PEER_TO_PEER, DEFAULT_INSTALL_DIR);
  }

  public TomcatInstall(TomcatVersion version, String installDir) throws Exception {
    this(version, TomcatConfig.PEER_TO_PEER, installDir);
  }

  public TomcatInstall(TomcatVersion version, TomcatConfig config) throws Exception {
    this(version, config, DEFAULT_INSTALL_DIR);
  }

  public TomcatInstall(TomcatVersion version, TomcatConfig config, String installDir)
      throws Exception {
    // Does download and install from URL
    super(installDir, version.downloadURL());

    this.config = config;
    this.version = version;

    // Get tomcat module path
    tomcatModulePath = findAndExtractModule(GEODE_BUILD_HOME, "tomcat");
    // Default properties
    setCacheProperty("enableLocalCache", "false");

    // Install geode sessions into tomcat install
    copyTomcatGeodeReqFiles(GEODE_BUILD_HOME + "/lib/");

    // Add required jars copied to jar skips so container startup is faster
    if (version.jarSkipPropertyName() != null) {
      updateProperties();
    }
  }

  /**
   * Sets the XML file to use for cache settings
   *
   * Calls {@link ContainerInstall#setCacheXMLFile(String, String)} with the default original cache
   * file located in the conf folder of the {@link #tomcatModulePath} and the given newXMLFilePath.
   */
  @Override
  public void setupCacheXMLFile(String newXMLFilePath) throws IOException {
    super.setCacheXMLFile(tomcatModulePath + "/conf/" + config.getXMLFile(), newXMLFilePath);
  }

  /**
   * Copies jars specified by {@link #tomcatRequiredJars} from the {@link #tomcatModulePath} and the
   * specified other directory passed to the function.
   * 
   * @throws IOException if the {@link #tomcatModulePath}, installation lib directory, or extra
   *         directory passed in contain no files.
   */
  private void copyTomcatGeodeReqFiles(String extraJarsPath) throws IOException {
    ArrayList<File> requiredFiles = new ArrayList<>();
    // The library path for the current tomcat installation
    String tomcatLibPath = getInstallPath() + "/lib/";

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
      for (File file : (new File(tomcatModulePath + "/lib/")).listFiles()) {
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
          "No files found in tomcat module directory " + tomcatModulePath + "/lib/");
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

    editPropertyFile(getInstallPath() + "/conf/catalina.properties", version.jarSkipPropertyName(),
        jarsToSkip, true);
  }

  /**
   * Build a HashMap with server property attributes for the server.xml file
   *
   * Server properties are obtained by iterating through {@link ContainerInstall#systemProperties}
   */
  private HashMap<String, String> buildServerXMLAttributes() throws IOException {
    HashMap<String, String> attributes = config.getRequiredXMLAttributes();

    for (String property : systemProperties.keySet())
      attributes.put(property, systemProperties.get(property));

    return attributes;
  }

  /**
   * Build a HashMap with cache property attributes for the context.xml file
   *
   * Cache properties are obtained by iterating through {@link ContainerInstall#cacheProperties}
   */
  private HashMap<String, String> buildContextXMLAttributes() {
    HashMap<String, String> attributes = version.getRequiredXMLAttributes();

    for (String property : cacheProperties.keySet())
      attributes.put(property, cacheProperties.get(property));

    return attributes;
  }

  /**
   * Update the server and context XML files
   *
   * Uses the {@link #buildContextXMLAttributes()} and {@link #buildServerXMLAttributes()} methods
   * to update to the proper attributes and values
   */
  private void updateXMLFiles() throws IOException {
    editXMLFile(getInstallPath() + "/conf/server.xml", "Tomcat", "Listener", "Server",
        buildServerXMLAttributes());
    editXMLFile(getInstallPath() + "/conf/context.xml", "Tomcat", "Manager", "Context",
        buildContextXMLAttributes());
  }

  /**
   * Tomcat specific property updater
   *
   * Overrides {@link ContainerInstall#writeProperties}. Most properties for Tomcat installs are
   * changed within the server.xml and context.xml files so this runs {@link #updateXMLFiles}.
   */
  @Override
  public void writeProperties() throws Exception {
    updateXMLFiles();
  }

  /**
   * Sets the address and port of the locator for this tomcat installation
   *
   * For Client Server installations the cache-client.xml file is updated within the installations
   * conf folder. For Peer to Peer installations the server.xml file is updated using
   * {@link #updateXMLFiles()}.
   */
  @Override
  public void setLocator(String address, int port) throws Exception {
    if (config == TomcatConfig.CLIENT_SERVER) {
      HashMap<String, String> attributes = new HashMap<>();
      attributes.put("host", address);
      attributes.put("port", Integer.toString(port));

      editXMLFile(tomcatModulePath + "/conf/" + config.getXMLFile(), "locator", "pool", attributes,
          true);

    } else {
      setSystemProperty("locators", address + "[" + port + "]");
    }

    logger.info("Set locator for Tomcat install to " + address + "[" + port + "]");
  }

  public TomcatVersion getVersion() {
    return version;
  }

  /**
   * @see ContainerInstall#getContainerId()
   */
  @Override
  public String getContainerId() {
    return "tomcat" + version.toInteger() + "x";
  }

  /**
   * @see ContainerInstall#getContainerDescription()
   */
  @Override
  public String getContainerDescription() {
    return version.name() + "_" + config.name();
  }

  /**
   * Modifies the tomcat configuration so that this points to the correct context.xml file when
   * setup and run using Cargo.
   */
  @Override
  public void modifyConfiguration(LocalConfiguration configuration) {
    // Copy context.xml file for actual server to get DeltaSessionManager as manager
    FileConfig contextConfigFile = new FileConfig();
    contextConfigFile.setToDir("conf");
    contextConfigFile.setFile(getInstallPath() + "/conf/context.xml");
    configuration.setConfigFileProperty(contextConfigFile);
  }
}
