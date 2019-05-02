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

package org.apache.geode.management.api;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.configuration.MemberConfig;
import org.apache.geode.management.configuration.RuntimeCacheElement;
import org.apache.geode.management.configuration.RuntimeRegionConfig;

/**
 * this is responsible for applying and persisting cache configuration changes on locators and/or
 * servers.
 */
@Experimental
public interface ClusterManagementService {

  String FEATURE_FLAG = "gemfire.enable-experimental-cluster-management-service";

  /**
   * This method will create the element on all the applicable members in the cluster and persist
   * the configuration in the cluster configuration if persistence is enabled.
   *
   * @param config this holds the configuration attributes of the element to be created on the
   *        cluster, as well as the group this config belongs to
   * @see CacheElement
   */
  ClusterManagementResult create(CacheElement config);

  /**
   * This method will delete the element on all the applicable members in the cluster and update the
   * configuration in the cluster configuration if persistence is enabled.
   *
   * @param config this holds the configuration attributes of the element to be deleted on the
   *        cluster
   * @throws IllegalArgumentException, NoMemberException, EntityExistsException
   * @see CacheElement
   */
  ClusterManagementResult delete(CacheElement config);

  /**
   * This method will update the element on all the applicable members in the cluster and persist
   * the updated configuration in the cluster configuration if persistence is enabled.
   *
   * @param config this holds the configuration attributes of the element to be updated on the
   *        cluster
   * @throws IllegalArgumentException, NoMemberException, EntityExistsException
   * @see CacheElement
   */
  ClusterManagementResult update(CacheElement config);

  /**
   * Perform a query for a given type of component in the cluster. This method uses the type of the
   * {@code filter} parameter to determine the component to query for. For example to query for all
   * regions, pass in an empty {@link RegionConfig} object. Setting various attributes on the filter
   * object acts as a predicate. Current supported fields are {@code id} and {@code group}. For
   * example:
   *
   * <pre>
   * RegionConfig filter = new RegionConfig();
   * // equivalent to using setName()
   * filter.setId("accounts-region");
   * ClusterManagementResult<RuntimeRegionConfig> result = cms.list(filter,
   *     RuntimeRegionConfig.class);
   * </pre>
   *
   * @param filter an object that determines the component to query for - For example {@link
   *        RegionConfig}
   *
   * @param runtimeConfigType the corresponding runtime return type. Every config object that can be
   *        used for filtering will have a corresponding runtime type that defines the runtime
   *        attributes
   *        for that config object. For example. {@code RegionConfig}'s runtime type is {@link
   *        RuntimeRegionConfig}. Some types may have the same object as the runtime type, for
   *        example
   *        {@link MemberConfig}.
   * @throws IllegalArgumentException if <code>runtimeConfigType</code> is not a subclass of
   *         <code>filter</code>
   */
  <T extends CacheElement, R extends RuntimeCacheElement> ClusterManagementResult<R> list(
      T filter, Class<R> runtimeConfigType);

  /**
   * Test to see if this instance of ClusterManagementService retrieved from the client side is
   * properly connected to the locator or not
   *
   * @return true if connected
   */
  boolean isConnected();

}
