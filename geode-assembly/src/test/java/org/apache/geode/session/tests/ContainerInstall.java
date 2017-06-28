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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.Logger;
import org.codehaus.cargo.container.configuration.LocalConfiguration;
import org.codehaus.cargo.container.deployable.WAR;
import org.codehaus.cargo.container.installer.Installer;
import org.codehaus.cargo.container.installer.ZipURLInstaller;
import org.codehaus.cargo.container.property.LoggingLevel;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.configuration.utils.ZipUtils;

/**
 * Base class for handling downloading and configuring J2EE containers.
 *
 * This class contains common logic for downloading and configuring J2EE containers with cargo, and
 * some common methods for applying geode session replication configuration to those containers.
 *
 * Subclasses provide installation of specific containers.
 */
public abstract class ContainerInstall {
  public static final Logger logger = LogService.getLogger();

  private final String INSTALL_PATH;
  public static final String DEFAULT_INSTALL_DIR = "/tmp/cargo_containers/";
  public static final String GEODE_BUILD_HOME = System.getenv("GEODE_HOME");

  public HashMap<String, String> cacheProperties;
  public HashMap<String, String> systemProperties;

  public ContainerInstall(String installDir, String downloadURL) throws MalformedURLException {
    logger.info("Installing container from URL " + downloadURL);

    // Optional step to install the container from a URL pointing to its distribution
    Installer installer = new ZipURLInstaller(new URL(downloadURL), "/tmp/downloads", installDir);
    installer.install();
    INSTALL_PATH = installer.getHome();
    logger.info("Installed container into " + getInstallPath());

    cacheProperties = new HashMap<>();
    systemProperties = new HashMap<>();
  }

  /**
   * The directory in which this container is installed.
   */
  public String getInstallPath() {
    return INSTALL_PATH;
  }

  /**
   * Called by the installation before container startup
   *
   * This is mainly used to write properties to whatever format they need to be in for a given
   * container before the container is started. The reason for doing this is to make sure that
   * expensive property updates (such as writing to file or building files from the command line)
   * only happen as often as they are needed. These kinds of updates usually only need to happen on
   * container startup or addition.
   */
  public abstract void writeProperties() throws Exception;

  /**
   * Cargo's specific string to identify the container
   */
  public abstract String getContainerId();

  /**
   * A human readable description of the container
   */
  public abstract String getContainerDescription();

  /**
   * Configure the geode session replication install in this container to connect to the given
   * locator.
   */
  public abstract void setLocator(String address, int port) throws Exception;

  /**
   * Sets the XML file which contains cache properties.
   *
   * Normally this XML file would be set to the cache-client.xml or cache-peer.xml files located in
   * the module's conf directory (located in build/install/apache-geode/tools/Modules/... for
   * geode-assembly). However, this allows containers to have different XML files so that locators
   * will not accidentally overwrite each other's when tests are run concurrently.
   *
   * The originalXMLFilePath is used to copy the original XML file to the newXMLFilePath so that all
   * settings previously there are saved and copied over.
   */
  public void setCacheXMLFile(String originalXMLFilePath, String newXMLFilePath)
      throws IOException {
    File moduleXMLFile = new File(originalXMLFilePath);
    File installXMLFile = new File(newXMLFilePath);

    installXMLFile.getParentFile().mkdirs();
    FileUtils.copyFile(moduleXMLFile, installXMLFile);

    setSystemProperty("cache-xml-file", installXMLFile.getAbsolutePath());
  }

  /**
   * Set a geode session replication property. For example enableLocalCache.
   */
  public String setCacheProperty(String name, String value) throws IOException {
    return cacheProperties.put(name, value);
  }

  /**
   * Set geode distributed system property.
   */
  public String setSystemProperty(String name, String value) throws IOException {
    return systemProperties.put(name, value);
  }

  /**
   * Get the specified cache property for an install
   */
  public String getCacheProperty(String name) {
    return cacheProperties.get(name);
  }

  /**
   * Get the specified system property for an install
   */
  public String getSystemProperty(String name) {
    return systemProperties.get(name);
  }

  /**
   * Callback to allow this install to update the configuration before it is launched
   */
  public void modifyConfiguration(LocalConfiguration configuration) {}

  protected String findSessionTestingWar() {
    // Start out searching directory above current
    String curPath = "../";

    // Looking for extensions folder
    final String warModuleDirName = "extensions";
    File warModuleDir = null;

    // While directory searching for is not found
    while (warModuleDir == null) {
      // Try to find the find the directory in the current directory
      File[] files = new File(curPath).listFiles();
      for (File file : files) {
        if (file.isDirectory() && file.getName().equals(warModuleDirName)) {
          warModuleDir = file;
          break;
        }
      }

      // Keep moving up until you find it
      curPath += "../";
    }

    // Return path to extensions plus hardcoded path from there to the WAR
    return warModuleDir.getAbsolutePath()
        + "/session-testing-war/build/libs/session-testing-war.war";
  }

  /**
   * Return the session testing war file to use for this container.
   *
   * This should be the war generated by the extensions/session-testing-war. For
   * {@link GenericAppServerInstall} this war is modified to include the geode session replication
   * components.
   */
  public WAR getDeployableWAR() {
    return new WAR(findSessionTestingWar());
  }

  protected static String findAndExtractModule(String geodeBuildHome, String moduleName)
      throws IOException {
    String modulePath = null;
    String modulesDir = geodeBuildHome + "/tools/Modules/";

    boolean archive = false;
    logger.info("Trying to access build dir " + modulesDir);

    // Search directory for tomcat module folder/zip
    for (File file : (new File(modulesDir)).listFiles()) {

      if (file.getName().toLowerCase().contains(moduleName)) {
        modulePath = file.getAbsolutePath();

        archive = !file.isDirectory();
        if (!archive)
          break;
      }
    }

    // Unzip if it is a zip file
    if (archive) {
      if (!FilenameUtils.getExtension(modulePath).equals("zip")) {
        throw new IOException("Bad module archive " + modulePath);
      }

      ZipUtils.unzip(modulePath, modulePath.substring(0, modulePath.length() - 4));

      modulePath = modulePath.substring(0, modulePath.length() - 4);
    }

    // No module found within directory throw IOException
    if (modulePath == null)
      throw new IOException("No module found in " + modulesDir);
    return modulePath;
  }

  /**
   * Edits the specified property within the given property file
   *
   * @param filePath path to the property file
   * @param propertyName property name to edit
   * @param propertyValue new property value
   * @param append whether or not to append the given property value. If true appends the given
   *        property value the current value. If false, replaces the current property value with the
   *        given property value
   */
  public void editPropertyFile(String filePath, String propertyName, String propertyValue,
      boolean append) throws Exception {
    FileInputStream input = new FileInputStream(filePath);
    Properties properties = new Properties();
    properties.load(input);

    String val;
    if (append)
      val = properties.getProperty(propertyName) + propertyValue;
    else
      val = propertyValue;

    properties.setProperty(propertyName, val);
    properties.store(new FileOutputStream(filePath), null);

    logger.info("Modified container Property file " + filePath);
  }

  protected void editXMLFile(String XMLPath, String tagId, String tagName, String parentTagName,
      HashMap<String, String> attributes) {
    editXMLFile(XMLPath, tagId, tagName, parentTagName, attributes, false);
  }

  protected void editXMLFile(String XMLPath, String tagName, String parentTagName,
      HashMap<String, String> attributes) {
    editXMLFile(XMLPath, null, tagName, parentTagName, attributes, false);
  }

  protected void editXMLFile(String XMLPath, String tagName, String parentTagName,
      HashMap<String, String> attributes, boolean writeOnSimilarAttributeNames) {
    editXMLFile(XMLPath, null, tagName, parentTagName, attributes, writeOnSimilarAttributeNames);
  }

  /**
   * Edit the given xml file
   * 
   * @param XMLPath The path to the xml file to edit
   * @param tagId The id of tag to edit. If null, then this method will add a new xml element,
   *        unless writeOnSimilarAttributeNames is set to true.
   * @param tagName The name of the xml element to edit
   * @param parentTagName The parent element of the element we should edit
   * @param attributes the xml attributes for the element to edit
   * @param writeOnSimilarAttributeNames If true, find an existing element with the same set of
   *        attributes as the attributes parameter, and modifies the attributes of that element,
   *        rather than adding a new element. If false, create a new XML element (unless tagId is
   *        not null).
   */
  protected void editXMLFile(String XMLPath, String tagId, String tagName, String parentTagName,
      HashMap<String, String> attributes, boolean writeOnSimilarAttributeNames) {
    // Get XML file to edit
    try {
      DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
      Document doc = docBuilder.parse(XMLPath);

      boolean hasTag = false;
      NodeList nodes = doc.getElementsByTagName(tagName);

      // If tags with name were found search to find tag with proper tagId and update its fields
      if (nodes != null) {
        for (int i = 0; i < nodes.getLength(); i++) {
          Node node = nodes.item(i);
          if (tagId != null) {
            Node idAttr = node.getAttributes().getNamedItem("id");
            // Check node for id attribute
            if (idAttr != null && idAttr.getTextContent().equals(tagId)) {
              NamedNodeMap nodeAttrs = node.getAttributes();

              // Remove previous attributes
              while (nodeAttrs.getLength() > 0) {
                nodeAttrs.removeNamedItem(nodeAttrs.item(0).getNodeName());
              }

              ((Element) node).setAttribute("id", tagId);
              // Set to new attributes
              for (String key : attributes.keySet()) {
                ((Element) node).setAttribute(key, attributes.get(key));
                // node.getAttributes().getNamedItem(key).setTextContent(attributes.get(key));
              }

              hasTag = true;
              break;
            }
          } else if (writeOnSimilarAttributeNames) {
            NamedNodeMap nodeAttrs = node.getAttributes();
            boolean updateNode = true;

            // Check to make sure has all attribute fields
            for (String key : attributes.keySet()) {
              if (nodeAttrs.getNamedItem(key) == null) {
                updateNode = false;
                break;
              }
            }
            // Check to make sure does not have more than attribute fields
            for (int j = 0; j < nodeAttrs.getLength(); j++) {
              if (attributes.get(nodeAttrs.item(j).getNodeName()) == null) {
                updateNode = false;
                break;
              }
            }

            // Update node attributes
            if (updateNode) {
              for (String key : attributes.keySet())
                node.getAttributes().getNamedItem(key).setTextContent(attributes.get(key));

              hasTag = true;
              break;
            }
          }

        }
      }

      if (!hasTag) {
        Element e = doc.createElement(tagName);
        // Set id attribute
        if (tagId != null)
          e.setAttribute("id", tagId);
        // Set other attributes
        for (String key : attributes.keySet())
          e.setAttribute(key, attributes.get(key));

        // Add it as a child of the tag for the file
        doc.getElementsByTagName(parentTagName).item(0).appendChild(e);
      }

      // Write updated XML file
      TransformerFactory transformerFactory = TransformerFactory.newInstance();
      Transformer transformer = transformerFactory.newTransformer();
      DOMSource source = new DOMSource(doc);
      StreamResult result = new StreamResult(new File(XMLPath));
      transformer.transform(source, result);

      logger.info("Modified container XML file " + XMLPath);
    } catch (Exception e) {
      throw new RuntimeException("Unable to edit XML file", e);
    }
  }

  /**
   * Get the location of this installations configuration home
   */
  public String getContainerConfigHome() {
    return "/tmp/cargo_configs/" + getContainerDescription();
  }

  /**
   * Get the logging level of this install
   */
  public String getLoggingLevel() {
    return LoggingLevel.HIGH.getLevel();
  }
}
