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

package org.apache.geode.cache.query.internal;

import java.util.Set;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.query.internal.xml.QueryMethodAuthorizerCreation;
import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
import org.apache.geode.internal.cache.CacheService;

public interface QueryConfigurationService extends CacheService {

  /**
   * Returns the currently configured {@link MethodInvocationAuthorizer} instance.
   *
   * @return the currently configured {@link MethodInvocationAuthorizer}
   */
  MethodInvocationAuthorizer getMethodAuthorizer();

  /**
   * Sets the configured {@link MethodInvocationAuthorizer} when creating the cache using a
   * declarative approach.
   *
   * @param cache the cache on which the {@link MethodInvocationAuthorizer} instance will be
   *        configured.
   * @param forceUpdate {@code true} to apply the configuration change even when there are
   *        continuous queries running, {@code false} otherwise.
   * @param creation the authorizer creation parameters.
   * @throws QueryConfigurationServiceException when there is an error while updating the new
   *         {@link MethodInvocationAuthorizer}.
   */
  void updateMethodAuthorizer(Cache cache, boolean forceUpdate,
      QueryMethodAuthorizerCreation creation) throws QueryConfigurationServiceException;

  /**
   * Updates the configured {@link MethodInvocationAuthorizer}.
   *
   * @param cache the cache on which the {@link MethodInvocationAuthorizer} instance will be
   *        configured.
   * @param forceUpdate {@code true} to apply the configuration change even when there are
   *        continuous queries running, {@code false} otherwise.
   * @param className the fully qualified name of the class that will be created and configured as
   *        the new {@link MethodInvocationAuthorizer}.
   * @param parameters the set of parameters that will be used to initialize the new
   *        {@link MethodInvocationAuthorizer}.
   * @throws QueryConfigurationServiceException when there is an error while updating the new
   *         {@link MethodInvocationAuthorizer}.
   */
  void updateMethodAuthorizer(Cache cache, boolean forceUpdate, String className,
      Set<String> parameters) throws QueryConfigurationServiceException;
}
