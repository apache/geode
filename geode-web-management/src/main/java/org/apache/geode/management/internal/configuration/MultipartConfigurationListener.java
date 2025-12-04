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
package org.apache.geode.management.internal.configuration;

import jakarta.servlet.MultipartConfigElement;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;
import jakarta.servlet.ServletRegistration;

/**
 * ServletContextListener that programmatically configures multipart file upload support
 * for the Management REST API DispatcherServlet.
 *
 * <p>
 * <b>Background:</b> This listener replaces the {@code <multipart-config>} element that was
 * previously declared in web.xml. The web.xml configuration was removed in commit 3ef6c393e0
 * because it caused Spring MVC to treat ALL HTTP requests as multipart requests, which broke
 * Spring Shell's custom parameter converters (e.g., PoolPropertyConverter for
 * {@code create data-source --pool-properties} commands).
 *
 * <p>
 * <b>Why Programmatic Configuration:</b> By configuring multipart support programmatically
 * via {@link ServletRegistration.Dynamic#setMultipartConfig}, we ensure that:
 * <ul>
 * <li>Jetty can parse multipart/form-data requests for JAR/config file uploads</li>
 * <li>Spring Shell's parameter binding remains unaffected (multipart only enabled at servlet
 * level, not globally)</li>
 * <li>The {@link org.apache.geode.management.internal.configuration.MultipartConfig} bean's
 * StandardServletMultipartResolver can properly read file size limits from the servlet
 * MultipartConfigElement</li>
 * </ul>
 *
 * <p>
 * <b>Configuration Values:</b> The multipart configuration matches the original web.xml
 * settings from commit 43e0daf34d:
 * <ul>
 * <li>Max file size: 50 MB (52,428,800 bytes)</li>
 * <li>Max request size: 50 MB (52,428,800 bytes)</li>
 * <li>File size threshold: 0 bytes (all uploads stored to disk immediately)</li>
 * </ul>
 *
 * <p>
 * <b>Servlet Container Integration:</b> This listener is registered programmatically in
 * {@link org.apache.geode.internal.cache.http.service.InternalHttpService#addWebApplication}
 * with {@code Source.EMBEDDED} to ensure it executes during ServletContext initialization,
 * before the DispatcherServlet starts.
 *
 * <p>
 * <b>Related Classes:</b>
 * <ul>
 * <li>{@link MultipartConfig} - Spring bean providing StandardServletMultipartResolver</li>
 * <li>{@link org.apache.geode.management.internal.rest.controllers.DeploymentManagementController}
 * - Uses multipart for JAR file uploads</li>
 * </ul>
 *
 * @see ServletRegistration.Dynamic#setMultipartConfig(MultipartConfigElement)
 * @see MultipartConfig
 * @since GemFire 1.0 (Jakarta EE 10 migration)
 */
public class MultipartConfigurationListener implements ServletContextListener {

  /**
   * Maximum size in bytes for uploaded files. Set to 50 MB to accommodate large JAR deployments.
   */
  private static final long MAX_FILE_SIZE = 52_428_800L; // 50 MB

  /**
   * Maximum size in bytes for multipart/form-data requests. Set to 50 MB to match max file size.
   */
  private static final long MAX_REQUEST_SIZE = 52_428_800L; // 50 MB

  /**
   * File size threshold in bytes for storing uploads in memory vs. disk. Set to 0 to always
   * write to disk immediately, avoiding out-of-memory issues with large JAR files.
   */
  private static final int FILE_SIZE_THRESHOLD = 0; // Always write to disk

  /**
   * Name of the DispatcherServlet as declared in web.xml.
   */
  private static final String SERVLET_NAME = "management";

  /**
   * Called when the ServletContext is initialized. Programmatically configures multipart
   * support for the DispatcherServlet.
   *
   * @param sce the ServletContextEvent containing the ServletContext being initialized
   * @throws IllegalStateException if the servlet registration cannot be found or configured
   */
  @Override
  public void contextInitialized(ServletContextEvent sce) {
    ServletContext servletContext = sce.getServletContext();

    // Get the existing servlet registration for the DispatcherServlet
    ServletRegistration servletRegistration = servletContext.getServletRegistration(SERVLET_NAME);

    if (servletRegistration == null) {
      throw new IllegalStateException(
          "Cannot configure multipart: servlet '" + SERVLET_NAME + "' not found. "
              + "This listener must execute after the DispatcherServlet is registered in web.xml.");
    }

    // Attempt to cast to Dynamic interface for configuration
    if (!(servletRegistration instanceof ServletRegistration.Dynamic)) {
      throw new IllegalStateException(
          "Cannot configure multipart: servlet '" + SERVLET_NAME
              + "' registration does not support dynamic configuration. "
              + "ServletRegistration type: " + servletRegistration.getClass().getName());
    }

    ServletRegistration.Dynamic dynamicRegistration =
        (ServletRegistration.Dynamic) servletRegistration;

    // Create and apply multipart configuration
    MultipartConfigElement multipartConfig = new MultipartConfigElement(
        null, // location (temp directory) - use container default
        MAX_FILE_SIZE,
        MAX_REQUEST_SIZE,
        FILE_SIZE_THRESHOLD);

    dynamicRegistration.setMultipartConfig(multipartConfig);

    servletContext.log(
        "Multipart configuration applied to servlet '" + SERVLET_NAME + "': "
            + "maxFileSize=" + MAX_FILE_SIZE + " bytes, "
            + "maxRequestSize=" + MAX_REQUEST_SIZE + " bytes, "
            + "fileSizeThreshold=" + FILE_SIZE_THRESHOLD + " bytes");
  }

  /**
   * Called when the ServletContext is about to be shut down. No cleanup needed.
   *
   * @param sce the ServletContextEvent containing the ServletContext being destroyed
   */
  @Override
  public void contextDestroyed(ServletContextEvent sce) {
    // No cleanup required
  }
}
