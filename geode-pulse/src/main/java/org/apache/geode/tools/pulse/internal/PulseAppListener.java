/*
 *
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

package org.apache.geode.tools.pulse.internal;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.function.BiFunction;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.geode.tools.pulse.internal.controllers.PulseController;
import org.apache.geode.tools.pulse.internal.data.PulseConstants;
import org.apache.geode.tools.pulse.internal.data.Repository;

/**
 * This class is used for checking the application running mode i.e. Embedded or not
 *
 * @since GemFire version 7.0.Beta 2012-09-23
 *
 */
// @WebListener
public class PulseAppListener implements ServletContextListener {
  private static final Logger logger = LogManager.getLogger();
  private static final String GEODE_SSLCONFIG_SERVLET_CONTEXT_PARAM = "org.apache.geode.sslConfig";

  private final boolean isEmbedded;
  private final Repository repository;
  private final ResourceBundle resourceBundle;
  private final BiFunction<String, ResourceBundle, Properties> propertiesFileLoader;

  public PulseAppListener() {
    this(Boolean.getBoolean(PulseConstants.SYSTEM_PROPERTY_PULSE_EMBEDDED), Repository.get(),
        PulseAppListener::loadPropertiesFromFile);
  }

  public PulseAppListener(boolean isEmbedded, Repository repository,
      BiFunction<String, ResourceBundle, Properties> propertiesFileLoader) {
    this.isEmbedded = isEmbedded;
    this.repository = repository;
    this.resourceBundle = repository.getResourceBundle();
    this.propertiesFileLoader = propertiesFileLoader;
  }

  @Override
  public void contextDestroyed(ServletContextEvent event) {

    // Stop all running threads those are created in Pulse
    // Stop cluster threads
    repository.removeAllClusters();

    logger.info("{}{}", resourceBundle.getString("LOG_MSG_CONTEXT_DESTROYED"),
        event.getServletContext().getContextPath());
  }

  @Override
  public void contextInitialized(ServletContextEvent event) {
    logger.info(resourceBundle.getString("LOG_MSG_CONTEXT_INITIALIZED"));

    // Load Pulse version details
    loadPulseVersionDetails();

    logger.info(resourceBundle.getString("LOG_MSG_CHECK_APP_RUNNING_MODE"));

    if (isEmbedded) {
      // jmx connection parameters
      logger.info(resourceBundle.getString("LOG_MSG_APP_RUNNING_EMBEDDED_MODE"));
      repository.setJmxUseLocator(false);
      repository.setHost(System.getProperty(PulseConstants.SYSTEM_PROPERTY_PULSE_HOST,
          PulseConstants.GEMFIRE_DEFAULT_HOST));
      repository.setPort(System.getProperty(PulseConstants.SYSTEM_PROPERTY_PULSE_PORT,
          PulseConstants.GEMFIRE_DEFAULT_PORT));

      repository.setUseSSLManager(
          Boolean.valueOf(System.getProperty(PulseConstants.SYSTEM_PROPERTY_PULSE_USESSL_MANAGER)));
      repository.setUseSSLLocator(
          Boolean.valueOf(System.getProperty(PulseConstants.SYSTEM_PROPERTY_PULSE_USESSL_LOCATOR)));

      Object sslProperties =
          event.getServletContext().getAttribute(GEODE_SSLCONFIG_SERVLET_CONTEXT_PARAM);
      if (sslProperties instanceof Properties) {
        repository.setJavaSslProperties((Properties) sslProperties);
      }
    } else {
      // jmx connection parameters
      logger.info(resourceBundle.getString("LOG_MSG_APP_RUNNING_NONEMBEDDED_MODE"));

      // Load Pulse Properties
      Properties pulseProperties =
          propertiesFileLoader.apply(PulseConstants.PULSE_PROPERTIES_FILE, resourceBundle);

      repository.setJmxUseLocator(Boolean.valueOf(
          pulseProperties.getProperty(PulseConstants.APPLICATION_PROPERTY_PULSE_USELOCATOR)));
      repository.setHost(pulseProperties.getProperty(PulseConstants.APPLICATION_PROPERTY_PULSE_HOST,
          PulseConstants.GEMFIRE_DEFAULT_HOST));
      repository.setPort(pulseProperties.getProperty(PulseConstants.APPLICATION_PROPERTY_PULSE_PORT,
          PulseConstants.GEMFIRE_DEFAULT_PORT));

      // SSL
      repository.setUseSSLManager(Boolean.valueOf(pulseProperties
          .getProperty(PulseConstants.SYSTEM_PROPERTY_PULSE_USESSL_MANAGER, "false")));
      repository.setUseSSLLocator(Boolean.valueOf(pulseProperties
          .getProperty(PulseConstants.SYSTEM_PROPERTY_PULSE_USESSL_LOCATOR, "false")));

      // load pulse security properties
      Properties pulseSecurityProperties =
          propertiesFileLoader.apply(PulseConstants.PULSE_SECURITY_PROPERTIES_FILE, resourceBundle);

      // set the ssl related properties found in pulsesecurity.properties
      if (!pulseSecurityProperties.isEmpty()) {
        Set<Entry<Object, Object>> entrySet = pulseSecurityProperties.entrySet();
        for (Entry<Object, Object> entry : entrySet) {
          String key = (String) entry.getKey();
          if (key.startsWith("javax.net.ssl.")) {
            String val = (String) entry.getValue();
            System.setProperty(key, val);
          }
        }

        repository.setJavaSslProperties(pulseSecurityProperties);
      }
    }
  }

  // Function to load pulse version details from properties file
  private void loadPulseVersionDetails() {
    Properties properties =
        propertiesFileLoader.apply(PulseConstants.PULSE_VERSION_PROPERTIES_FILE, resourceBundle);
    // Set pulse version details in common object
    PulseController.pulseVersion
        .setPulseVersion(properties.getProperty(PulseConstants.PROPERTY_PULSE_VERSION, ""));
    PulseController.pulseVersion
        .setPulseBuildId(properties.getProperty(PulseConstants.PROPERTY_BUILD_ID, ""));
    PulseController.pulseVersion
        .setPulseBuildDate(properties.getProperty(PulseConstants.PROPERTY_BUILD_DATE, ""));
    PulseController.pulseVersion
        .setPulseSourceDate(properties.getProperty(PulseConstants.PROPERTY_SOURCE_DATE, ""));
    PulseController.pulseVersion.setPulseSourceRevision(
        properties.getProperty(PulseConstants.PROPERTY_SOURCE_REVISION, ""));
    PulseController.pulseVersion.setPulseSourceRepository(
        properties.getProperty(PulseConstants.PROPERTY_SOURCE_REPOSITORY, ""));
    logger.info(PulseController.pulseVersion.getPulseVersionLogMessage());
  }

  // Function to load pulse properties from pulse.properties file
  private static Properties loadPropertiesFromFile(String propertyFile,
      ResourceBundle resourceBundle) {
    final Properties properties = new Properties();
    try (final InputStream stream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(propertyFile)) {
      logger.info(propertyFile + " " + resourceBundle.getString("LOG_MSG_FILE_FOUND"));
      properties.load(stream);
    } catch (IOException e) {
      logger.error(resourceBundle.getString("LOG_MSG_EXCEPTION_LOADING_PROPERTIES_FILE"), e);
    }

    return properties;
  }
}
