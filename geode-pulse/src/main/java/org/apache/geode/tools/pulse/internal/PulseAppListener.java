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
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.Set;

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
  private final ResourceBundle resourceBundle = Repository.get().getResourceBundle();
  private static final String GEODE_SSLCONFIG_SERVLET_CONTEXT_PARAM = "org.apache.geode.sslConfig";

  private Properties pulseProperties;
  private Properties pulseSecurityProperties;

  @Override
  public void contextDestroyed(ServletContextEvent event) {

    // Stop all running threads those are created in Pulse
    // Stop cluster threads
    Repository.get().removeAllClusters();

    logger.info("{}{}", resourceBundle.getString("LOG_MSG_CONTEXT_DESTROYED"),
        event.getServletContext().getContextPath());
  }

  @Override
  public void contextInitialized(ServletContextEvent event) {
    logger.info(resourceBundle.getString("LOG_MSG_CONTEXT_INITIALIZED"));
    // Load Pulse Properties
    pulseProperties = loadProperties(PulseConstants.PULSE_PROPERTIES_FILE);

    // Load Pulse version details
    loadPulseVersionDetails();

    // load pulse security properties
    pulseSecurityProperties = loadProperties(PulseConstants.PULSE_SECURITY_PROPERTIES_FILE);

    // Reference to repository
    Repository repository = Repository.get();

    logger.info(resourceBundle.getString("LOG_MSG_CHECK_APP_RUNNING_MODE"));

    boolean sysIsEmbedded = Boolean.getBoolean(PulseConstants.SYSTEM_PROPERTY_PULSE_EMBEDDED);

    if (sysIsEmbedded) {
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

      // set the ssl related properties found in pulsesecurity.properties
      if (!pulseSecurityProperties.isEmpty()) {
        Set entrySet = pulseSecurityProperties.entrySet();
        for (Iterator it = entrySet.iterator(); it.hasNext();) {
          Entry<String, String> entry = (Entry<String, String>) it.next();
          String key = entry.getKey();
          if (key.startsWith("javax.net.ssl.")) {
            String val = entry.getValue();
            System.setProperty(key, val);
          }
        }
      }
    }
  }

  // Function to load pulse version details from properties file
  private void loadPulseVersionDetails() {
    Properties properties = loadProperties(PulseConstants.PULSE_VERSION_PROPERTIES_FILE);
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
  private Properties loadProperties(String propertyFile) {
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
