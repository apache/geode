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

import java.util.Map.Entry;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.Set;

import javax.servlet.ServletContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.tools.pulse.internal.controllers.PulseController;
import org.apache.geode.tools.pulse.internal.data.PulseConstants;
import org.apache.geode.tools.pulse.internal.data.Repository;

/**
 * This class is used for checking the application running mode i.e. Embedded or not
 *
 * @since GemFire version 7.0.Beta 2012-09-23
 */
@Component
public class PulseAppListener implements ApplicationListener<ApplicationEvent> {
  private static final Logger logger = LogManager.getLogger();
  private static final String GEODE_SSLCONFIG_SERVLET_CONTEXT_PARAM = "org.apache.geode.sslConfig";

  private final boolean isEmbedded;
  private final Repository repository;
  private final ResourceBundle resourceBundle;
  private final PropertiesFileLoader propertiesFileLoader;
  private final PulseController pulseController;

  @Autowired
  public PulseAppListener(PulseController pulseController, Repository repository,
      PropertiesFileLoader propertiesLoader) {
    this(Boolean.getBoolean(PulseConstants.SYSTEM_PROPERTY_PULSE_EMBEDDED),
        propertiesLoader, pulseController, repository);
  }

  public PulseAppListener(boolean isEmbedded, PropertiesFileLoader propertiesFileLoader,
      PulseController pulseController, Repository repository) {
    this.isEmbedded = isEmbedded;
    this.propertiesFileLoader = propertiesFileLoader;
    this.pulseController = pulseController;
    this.repository = repository;
    resourceBundle = repository.getResourceBundle();
  }

  @Override
  public void onApplicationEvent(ApplicationEvent event) {
    if (event instanceof ContextRefreshedEvent) {
      contextInitialized((ContextRefreshedEvent) event);
    } else if (event instanceof ContextClosedEvent) {
      contextDestroyed((ContextClosedEvent) event);
    }
  }

  public void contextInitialized(ContextRefreshedEvent event) {
    logger.info(resourceBundle.getString("LOG_MSG_CONTEXT_INITIALIZED"));

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
          Boolean.parseBoolean(
              System.getProperty(PulseConstants.SYSTEM_PROPERTY_PULSE_USESSL_MANAGER)));
      repository.setUseSSLLocator(
          Boolean.parseBoolean(
              System.getProperty(PulseConstants.SYSTEM_PROPERTY_PULSE_USESSL_LOCATOR)));

      WebApplicationContext applicationContext =
          (WebApplicationContext) event.getApplicationContext();
      ServletContext servletContext = applicationContext.getServletContext();

      Object sslProperties =
          servletContext.getAttribute(GEODE_SSLCONFIG_SERVLET_CONTEXT_PARAM);
      if (sslProperties instanceof Properties) {
        repository.setJavaSslProperties((Properties) sslProperties);
      }
    } else {
      // jmx connection parameters
      logger.info(resourceBundle.getString("LOG_MSG_APP_RUNNING_NONEMBEDDED_MODE"));

      // Load Pulse Properties
      Properties pulseProperties =
          propertiesFileLoader.loadProperties(PulseConstants.PULSE_PROPERTIES_FILE, resourceBundle);

      repository.setJmxUseLocator(Boolean.valueOf(
          pulseProperties.getProperty(PulseConstants.APPLICATION_PROPERTY_PULSE_USELOCATOR)));
      repository.setHost(pulseProperties.getProperty(PulseConstants.APPLICATION_PROPERTY_PULSE_HOST,
          PulseConstants.GEMFIRE_DEFAULT_HOST));
      repository.setPort(pulseProperties.getProperty(PulseConstants.APPLICATION_PROPERTY_PULSE_PORT,
          PulseConstants.GEMFIRE_DEFAULT_PORT));

      // SSL
      repository.setUseSSLManager(Boolean.parseBoolean(pulseProperties
          .getProperty(PulseConstants.SYSTEM_PROPERTY_PULSE_USESSL_MANAGER, "false")));
      repository.setUseSSLLocator(Boolean.parseBoolean(pulseProperties
          .getProperty(PulseConstants.SYSTEM_PROPERTY_PULSE_USESSL_LOCATOR, "false")));

      // load pulse security properties
      Properties pulseSecurityProperties =
          propertiesFileLoader
              .loadProperties(PulseConstants.PULSE_SECURITY_PROPERTIES_FILE, resourceBundle);

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

  public void contextDestroyed(ContextClosedEvent event) {

    // Stop all running threads those are created in Pulse
    // Stop cluster threads
    repository.removeAllClusters();

    WebApplicationContext applicationContext =
        (WebApplicationContext) event.getApplicationContext();
    ServletContext servletContext = applicationContext.getServletContext();

    logger.info("{}{}", resourceBundle.getString("LOG_MSG_CONTEXT_DESTROYED"),
        servletContext.getContextPath());
  }

  private void loadPulseVersionDetails() {
    Properties properties =
        propertiesFileLoader
            .loadProperties(PulseConstants.PULSE_VERSION_PROPERTIES_FILE, resourceBundle);
    pulseController.getPulseVersion()
        .setPulseVersion(properties.getProperty(PulseConstants.PROPERTY_PULSE_VERSION, ""));
    pulseController.getPulseVersion()
        .setPulseBuildId(properties.getProperty(PulseConstants.PROPERTY_BUILD_ID, ""));
    pulseController.getPulseVersion()
        .setPulseBuildDate(properties.getProperty(PulseConstants.PROPERTY_BUILD_DATE, ""));
    pulseController.getPulseVersion()
        .setPulseSourceDate(properties.getProperty(PulseConstants.PROPERTY_SOURCE_DATE, ""));
    pulseController.getPulseVersion().setPulseSourceRevision(
        properties.getProperty(PulseConstants.PROPERTY_SOURCE_REVISION, ""));
    pulseController.getPulseVersion().setPulseSourceRepository(
        properties.getProperty(PulseConstants.PROPERTY_SOURCE_REPOSITORY, ""));
    logger.info(pulseController.getPulseVersion().getPulseVersionLogMessage());
  }
}
