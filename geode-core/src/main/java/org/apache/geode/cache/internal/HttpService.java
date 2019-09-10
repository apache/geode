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
package org.apache.geode.cache.internal;

import java.nio.file.Path;
import java.util.Map;

import org.apache.geode.internal.cache.CacheService;
import org.apache.geode.internal.cache.InternalCache;

/**
 * This interface provides access to the simple http service exposed on the Geode
 * {@link InternalCache}. An instance can be retrieved by calling
 * {@code InternalCache.getService(HttpService.class)}.
 */
public interface HttpService extends CacheService {

  String SECURITY_SERVICE_SERVLET_CONTEXT_PARAM = "org.apache.geode.securityService";

  String CLUSTER_MANAGEMENT_SERVICE_CONTEXT_PARAM = "org.apache.geode.cluster.management.service";

  String GEODE_SSLCONFIG_SERVLET_CONTEXT_PARAM = "org.apache.geode.sslConfig";

  String AUTH_TOKEN_ENABLED_PARAM = "org.apache.geode.auth.token.enabled";

  /**
   * Add a new web application in the form of a war file. This method is also implicitly
   * responsible for starting the container if necessary.
   *
   * @param webAppContext the context path to be exposed for the web application
   * @param warFilePath the absolute path to the war file
   * @param attributeNameValuePairs attributes to be set on the servlet context
   */
  void addWebApplication(String webAppContext, Path warFilePath,
      Map<String, Object> attributeNameValuePairs) throws Exception;
}
