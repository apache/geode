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

import static java.nio.file.Files.copy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.function.IntSupplier;

import org.codehaus.cargo.container.configuration.FileConfig;
import org.codehaus.cargo.container.configuration.StandaloneLocalConfiguration;
import org.codehaus.cargo.container.tomcat.TomcatPropertySet;
import org.codehaus.cargo.util.XmlReplacement;


/**
 * Container for a tomcat installation
 *
 * Extends {@link ServerContainer} to form a basic container which sets up and configures a Tomcat
 * container.
 */
public class TomcatContainer extends ServerContainer {

  private final Path contextXMLFile;
  private final Path serverXMLFile;

  private static final String DEFAULT_TOMCAT_CONFIG_XML_DIR = "conf/";

  private static final String DEFAULT_TOMCAT_XML_REPLACEMENT_DIR =
      DEFAULT_TOMCAT_CONFIG_XML_DIR + "Catalina/localhost/";

  private static final String DEFAULT_TOMCAT_CONTEXT_XML_REPLACEMENT_NAME = "context.xml.default";

  /*
   * Setup the Tomcat container
   *
   * Sets up a configuration for the container using the specified installation and configuration
   * home. Finds and sets up the server.xml and context.xml files needed to specify container
   * properties, deploys the session testing WAR file to the Cargo container, and sets various
   * container properties (i.e. locator, local cache, etc.)
   */
  TomcatContainer(TomcatInstall install, Path rootDir, Path containerConfigHome,
      String containerDescriptors, IntSupplier portSupplier) throws IOException {
    super(install, rootDir, containerConfigHome, containerDescriptors, portSupplier);

    // Setup container specific XML files
    contextXMLFile = cargoLogDir.resolve("context.xml");
    serverXMLFile = defaultConfigDir.resolve("server.xml");

    // Copy the default container context XML file from the install to the specified path
    copy(defaultConfigDir.resolve("context.xml"), contextXMLFile);
    // Set the container context XML file to the new location copied to above
    setConfigFile(contextXMLFile,
        DEFAULT_TOMCAT_XML_REPLACEMENT_DIR,
        DEFAULT_TOMCAT_CONTEXT_XML_REPLACEMENT_NAME);

    if (install.getConnectionType() == ContainerInstall.ConnectionType.CLIENT_SERVER ||
        install.getConnectionType() == ContainerInstall.ConnectionType.CACHING_CLIENT_SERVER) {
      setCacheProperty("enableLocalCache",
          String.valueOf(install.getConnectionType().enableLocalCache()));
    }

    if (install.getCommitValve() != TomcatInstall.CommitValve.DEFAULT) {
      setCacheProperty("enableCommitValve", install.getCommitValve().getValue());
    }

    setCacheProperty("className", install.getContextSessionManagerClass());

    // Deploy war file to container configuration
    deployWar();
    // Setup the default installations locators
    setLocator(install.getDefaultLocatorAddress(), install.getDefaultLocatorPort());
  }

  /**
   * Get the AJP port of this container using {@link #getPort()} with the argument
   * {@link TomcatPropertySet#AJP_PORT}
   *
   * @return the AJP port of this container
   */
  public String getAJPPort() {
    return getPort(TomcatPropertySet.AJP_PORT);
  }

  /**
   * Implements the {@code ServerContainer#writeSettings()} function in order to write the proper
   * settings to the container
   *
   * <p>
   * Method uses the {@link ContainerInstall#editXMLFile(Path, String, String, String, HashMap)}
   * to edit the {@link #contextXMLFile} with the {@link #cacheProperties}. Method uses
   * {@link #writePropertiesToConfig(StandaloneLocalConfiguration, String, String, HashMap)} to
   * write the {@link #systemProperties} to the {@link #serverXMLFile} using the container's
   * configuration (obtained from {@link #getConfiguration()}).
   */
  @Override
  public void writeSettings() {
    StandaloneLocalConfiguration config = (StandaloneLocalConfiguration) getConfiguration();

    // Edit the context XML file
    ContainerInstall.editXMLFile(contextXMLFile, "Tomcat", "Manager", "Context",
        cacheProperties);
    writePropertiesToConfig(config,
        DEFAULT_TOMCAT_CONFIG_XML_DIR + "/" + serverXMLFile.toFile().getName(),
        "//Server/Listener[@className='"
            + ((TomcatInstall) getInstall()).getServerLifeCycleListenerClass() + "']",
        systemProperties);
  }

  /**
   * Edits the container's configuration so that the file's XML element specified by the XPath
   * parameter contains the given XML attributes
   *
   * Uses {@link XmlReplacement} instances to add XML attributes to the specified XML node without
   * actively updating the original XML file.
   *
   * This function is used to edit the system properties that need to be placed in the server.xml
   * file. Adding these replacement XML pieces to the container's configuration allows the
   * configuration to modify the server.xml file only when creating the standalone container
   * configuration. This means that the server.xml file located in the installation's 'conf' folder
   * remains static, which resolves possible concurrency issues that might arise if more than one
   * container is modifying the server.xml file.
   *
   * @param file The path to the XML file that will be edited
   * @param XPath The path within XML file that leads to the node that should be changed
   * @param attributes The attributes to add to the node
   */
  private void writePropertiesToConfig(StandaloneLocalConfiguration config, String file,
      String XPath, HashMap<String, String> attributes) {
    for (String key : attributes.keySet()) {
      XmlReplacement property = new XmlReplacement();
      property.setFile(file);
      property.setXpathExpression(XPath);
      property.setAttributeName(key);
      property.setValue(attributes.get(key));
      config.addXmlReplacement(property);
    }
  }

  /**
   * Sets a configuration file property for the container's configuration
   *
   * This function is currently only used to specify a different context.xml file from the one
   * located in the installations 'conf' folder.
   *
   * @param filePath The path to the new configuration file
   * @param configDirDest The name of the directory that the configuration file be placed in
   * @param configFileDestName The name of destination file for the new configuration file
   */
  private void setConfigFile(Path filePath, String configDirDest, String configFileDestName) {
    FileConfig configFile = new FileConfig();

    configFile.setFile(filePath.toString());
    configFile.setToDir(configDirDest);
    configFile.setToFile(configFileDestName);
    getConfiguration().setConfigFileProperty(configFile);
  }
}
