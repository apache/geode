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

import java.util.concurrent.CompletableFuture;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.configuration.AbstractConfiguration;
import org.apache.geode.management.runtime.OperationResult;
import org.apache.geode.management.runtime.RuntimeInfo;

/**
 * Implementations of this interface are responsible for applying and persisting cache
 * configuration changes.
 */
@Experimental
public interface ClusterManagementService extends AutoCloseable {
  /**
   * This method will create the element on all the applicable members in the cluster and persist
   * the configuration in the cluster configuration if persistence is enabled.
   *
   * @param config this holds the configuration attributes of the element to be created on the
   *        cluster, as well as the group this config belongs to
   * @return a {@link ClusterManagementRealizationResult} indicating the success of the creation
   * @throws ClusterManagementRealizationException if unsuccessful
   */
  <T extends AbstractConfiguration<?>> ClusterManagementRealizationResult create(T config);

  /**
   * This method will delete the element on all the applicable members in the cluster and update the
   * configuration in the cluster configuration if persistence is enabled.
   *
   * @param config this holds the name or id of the element to be deleted on the cluster
   * @return a {@link ClusterManagementRealizationResult} indicating the success of the deletion
   * @throws ClusterManagementRealizationException if unsuccessful
   */
  <T extends AbstractConfiguration<?>> ClusterManagementRealizationResult delete(T config);

  /**
   * This method will update the element on all the applicable members in the cluster and persist
   * the updated configuration in the cluster configuration if persistence is enabled.
   *
   * @param config this holds the configuration attributes of the element to be updated on the
   *        cluster, as well as the group this config belongs to
   * @return a {@link ClusterManagementRealizationResult} indicating the success of the update
   * @throws ClusterManagementRealizationException if unsuccessful
   */
  <T extends AbstractConfiguration<?>> ClusterManagementRealizationResult update(T config);

  /**
   * This method will list instances of the element type in the cluster configuration, along with
   * additional runtime information from cluster members
   *
   * @param filter the filterable attributes are used to identify the elements to list. Any
   *        non-filterable attributes will be ignored.
   * @return a {@link ClusterManagementListResult} holding a list of matching instances in
   *         {@link ClusterManagementListResult#getResult()}
   * @throws ClusterManagementException if unsuccessful
   */
  <T extends AbstractConfiguration<R>, R extends RuntimeInfo> ClusterManagementListResult<T, R> list(
      T filter);

  /**
   * This method will get a single instance of the element type in the cluster configuration, along
   * with additional runtime information from cluster members
   *
   * @param config this holds the name or id of the element to be retrieved
   * @return a {@link ClusterManagementGetResult}
   * @throws ClusterManagementException if unsuccessful or, no matching element is found, or
   *         multiple matches are found
   */
  <T extends AbstractConfiguration<R>, R extends RuntimeInfo> ClusterManagementGetResult<T, R> get(
      T config);

  /**
   * This method will launch a cluster management operation asynchronously.
   *
   * @param op the operation plus any parameters.
   * @param <A> the operation type (a subclass of {@link ClusterManagementOperation}
   * @param <V> the return type of the operation
   * @return a {@link ClusterManagementOperationResult} holding a {@link CompletableFuture} (if the
   *         operation was launched successfully) or an error code otherwise.
   * @throws ClusterManagementException if unsuccessful
   */
  <A extends ClusterManagementOperation<V>, V extends OperationResult> ClusterManagementOperationResult<V> start(
      A op);

  /**
   * Checks and returns the status of a given previously started operation.
   *
   * @param opId the operationId of a previously started operation
   * @param <V> the return type of the operation
   * @return the status of the identified operation
   */
  <V extends OperationResult> ClusterManagementOperationResult<V> checkStatus(
      String opId);

  /**
   * This method will list the status of all asynchronous cluster management operations in progress
   * or recently completed.
   *
   * @param <A> the operation type (a subclass of {@link ClusterManagementOperation}
   * @param <V> the return type of the operation
   * @param opType the operation type to list
   * @return a list of {@link ClusterManagementOperationResult}
   * @throws ClusterManagementException if unsuccessful
   */
  <A extends ClusterManagementOperation<V>, V extends OperationResult> ClusterManagementListOperationsResult<V> list(
      A opType);

  /**
   * Test to see if this instance of ClusterManagementService retrieved from the client side is
   * properly connected to the locator or not
   *
   * @return true if connected
   */
  boolean isConnected();

  /**
   * release any resources controlled by this service
   */
  @Override
  void close();
}
