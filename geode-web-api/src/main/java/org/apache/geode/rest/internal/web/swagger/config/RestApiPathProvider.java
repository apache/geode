/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.rest.internal.web.swagger.config;

import java.net.UnknownHostException;

import javax.servlet.ServletContext;

import com.mangofactory.swagger.paths.SwaggerPathProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.internal.net.SocketCreator;

@SuppressWarnings("unused")
public class RestApiPathProvider extends SwaggerPathProvider {

  @Autowired
  private ServletContext servletContext;

  private final String docsLocation;

  private SwaggerPathProvider defaultPathProvider;

  public RestApiPathProvider(final String docsLocation) {
    Assert.isTrue(!StringUtils.isBlank(docsLocation),
        "The docs location must be specified!");

    DistributionConfig config = InternalDistributedSystem.getAnyInstance().getConfig();
    String scheme = config.getHttpServiceSSLEnabled() ? "https" : "http";

    this.docsLocation = scheme + "://" + getBindAddressForHttpService() + ":" + config.getHttpServicePort();
  }

  private String getBindAddressForHttpService() {
    DistributionConfig config = InternalDistributedSystem.getAnyInstance().getConfig();
    java.lang.String bindAddress = config.getHttpServiceBindAddress();
    if (org.apache.commons.lang.StringUtils.isBlank(bindAddress)) {
      if (org.apache.commons.lang.StringUtils.isBlank(config.getServerBindAddress())) {
        if (org.apache.commons.lang.StringUtils.isBlank(config.getBindAddress())) {
          try {
          bindAddress = SocketCreator.getLocalHost().getHostAddress();
          } catch (UnknownHostException e) {
            e.printStackTrace();
          }
        } else {
          bindAddress = config.getBindAddress();
        }
      } else {
        bindAddress = config.getServerBindAddress();
      }
    }
    return bindAddress;
  }

  @Override
  public String getApiResourcePrefix() {
    return defaultPathProvider.getApiResourcePrefix();
  }

  /**
   * For relative SwaggerPathProviders this is typically '/' meaning relative to the swagger ui page serving the
   * documentation. The swagger specification recommends that this should be an absolute URL.
   * <p>
   * Corresponds to the base path attribute of a swagger api declaration.
   * This is the actual base path serving the api (not the swagger documentation)
   * @return the applications base uri
   */
  @Override
  protected String applicationPath() {
    return null;
  }

  /**
   * The base path to the swagger api documentation.
   * <p>
   * Typically docs are served from &lt;yourApp&gt;/api-docs so a relative resourceListing path will omit the api-docs
   * segment.
   * E.g.
   * Relative: "path": "/"
   * Absolute: "path": "http://localhost:8080/api-docs"
   * @return the documentation base path
   */
  @Override
  protected String getDocumentationPath() {
    return null;
  }

  //  @Override
  //  public String getAppBasePath() {
  //    return UriComponentsBuilder.fromHttpUrl(docsLocation)
  //        .path(servletContext.getContextPath()).build().toString();
  //  }
  //
  //  @Override
  //  public String getSwaggerDocumentationBasePath() {
  //    return UriComponentsBuilder.fromHttpUrl(getAppBasePath())
  //        .pathSegment("api-docs/").build().toString();
  //  }
  //
  //  @Override
  //  public String getRequestMappingEndpoint(String requestMappingPattern) {
  //    return defaultPathProvider.getRequestMappingEndpoint(requestMappingPattern);
  //  }

  public void setDefaultPathProvider(
      final SwaggerPathProvider defaultSwaggerPathProvider) {
    this.defaultPathProvider = defaultSwaggerPathProvider;
  }

}
