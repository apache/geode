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
package org.apache.geode.management.internal.rest;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.multipart.support.StandardServletMultipartResolver;

/**
 * Configuration for multipart file upload support.
 *
 * <p>
 * GEODE-10466: Configures multipart resolver programmatically instead of via web.xml
 * {@code <multipart-config>}. This prevents Spring MVC from treating ALL requests as multipart,
 * which would break Spring Shell 3.x parameter conversion for commands that use custom converters
 * (like PoolPropertyConverter for create data-source --pool-properties).
 *
 * <p>
 * With {@code StandardServletMultipartResolver}, Spring MVC only processes multipart requests when
 * the Content-Type header is "multipart/form-data", leaving other requests (like JDBC connector
 * commands with JSON-style parameters) to use normal Spring Shell parameter binding.
 *
 * <p>
 * Technical Background:
 * <ul>
 * <li>web.xml {@code <multipart-config>} causes DispatcherServlet to wrap ALL HttpServletRequests
 * as MultipartHttpServletRequests, changing how Spring MVC processes parameters</li>
 * <li>This breaks Spring Shell converters because multipart parameter processing bypasses
 *
 * @ShellOption validation and custom Converter beans</li>
 *              <li>StandardServletMultipartResolver only activates for actual multipart
 *              requests</li>
 *              <li>File size limits (50MB) are enforced at the application level via resolver
 *              configuration</li>
 *              </ul>
 *
 * @see org.springframework.web.multipart.support.StandardServletMultipartResolver
 * @since Geode 1.15.0
 */
@Configuration
public class MultipartConfig {

  /**
   * Configures multipart file upload resolver with 50MB size limits.
   *
   * <p>
   * This bean enables multipart file uploads for endpoints that need them (like create-mapping
   * with --pdx-class-file) while preserving normal parameter binding for other commands.
   *
   * <p>
   * <b>Servlet-Level Configuration:</b> The actual multipart configuration (file size limits,
   * temp directory, etc.) is set programmatically on the DispatcherServlet by
   * {@link org.apache.geode.management.internal.configuration.MultipartConfigurationListener},
   * which is registered in {@code InternalHttpService.addWebApplication()}. The listener
   * configures {@link jakarta.servlet.MultipartConfigElement} with 50MB limits via
   * {@link jakarta.servlet.ServletRegistration.Dynamic#setMultipartConfig}.
   *
   * @return configured multipart resolver that reads limits from servlet's MultipartConfigElement
   * @see org.apache.geode.management.internal.configuration.MultipartConfigurationListener
   */
  @Bean
  public StandardServletMultipartResolver multipartResolver() {
    // StandardServletMultipartResolver automatically reads configuration from the
    // jakarta.servlet.MultipartConfigElement set on the DispatcherServlet by
    // MultipartConfigurationListener. No additional configuration needed here.
    return new StandardServletMultipartResolver();
  }
}
