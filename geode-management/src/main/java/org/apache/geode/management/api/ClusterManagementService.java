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
  <T extends CacheElement & RespondsWith<R>, R extends CacheElement> ClusterManagementResult<T> create(
      T config);

  /**
   * This method will delete the element on all the applicable members in the cluster and update the
   * configuration in the cluster configuration if persistence is enabled.
   *
   * @param config this holds the configuration attributes of the element to be deleted on the
   *        cluster
   * @throws IllegalArgumentException, NoMemberException, EntityExistsException
   * @see CacheElement
   */
  <T extends CacheElement & RespondsWith<R>, R extends CacheElement> ClusterManagementResult<T> delete(
      T config);

  /**
   * This method will update the element on all the applicable members in the cluster and persist
   * the updated configuration in the cluster configuration if persistence is enabled.
   *
   * @param config this holds the configuration attributes of the element to be updated on the
   *        cluster
   * @throws IllegalArgumentException, NoMemberException, EntityExistsException
   * @see CacheElement
   */
  <T extends CacheElement & RespondsWith<R>, R extends CacheElement> ClusterManagementResult<T> update(
      T config);

  <T extends CacheElement & RespondsWith<R>, R extends CacheElement> ClusterManagementResult<R> list(
      T config);

  <T extends CacheElement & RespondsWith<R>, R extends CacheElement> ClusterManagementResult<R> get(
      T config);

  /**
   * Test to see if this instance of ClusterManagementService retrieved from the client side is
   * properly connected to the locator or not
   *
   * @return true if connected
   */
  boolean isConnected();

}
