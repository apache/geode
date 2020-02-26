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
 *
 */

package org.apache.geode.management.api;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.configuration.AbstractConfiguration;
import org.apache.geode.management.runtime.OperationResult;
import org.apache.geode.management.runtime.RuntimeInfo;

/**
 * Interface which abstracts the transport between the CMS client and the endpoint. Currently only
 * an http implementation exists. However it does allow for different implementations; perhaps
 * something that doesn't use {@code RestTemplate}. This interface supports the operations from
 * {@link ClusterManagementService}.
 */
@Experimental
public interface ClusterManagementServiceTransport {

  /**
   * Submit a message with a specific command. This supports the
   * {@link ClusterManagementService#create}
   * and {@link ClusterManagementService#delete} commands.
   *
   * @param configMessage configuration object
   * @param command the command to use
   * @param <T> configuration object which extends {@link AbstractConfiguration}
   * @return {@link ClusterManagementRealizationResult}
   */
  <T extends AbstractConfiguration<?>> ClusterManagementRealizationResult submitMessage(
      T configMessage, CommandType command);

  /**
   * Submit a message with a specific command which returns a list result. This supports the
   * {@link ClusterManagementService#list(AbstractConfiguration)} command.
   *
   * @param configMessage configuration object
   * @param <T> configuration object which extends {@link AbstractConfiguration}
   * @return {@link ClusterManagementListResult}
   */
  <T extends AbstractConfiguration<R>, R extends RuntimeInfo> ClusterManagementListResult<T, R> submitMessageForList(
      T configMessage);

  /**
   * Submit a message with a specific command which returns a single result. This supports the
   * {@link ClusterManagementService#get} command.
   *
   * @param configMessage configuration object
   * @param <T> configuration object which extends {@link AbstractConfiguration}
   * @return {@link ClusterManagementGetResult}
   */
  <T extends AbstractConfiguration<R>, R extends RuntimeInfo> ClusterManagementGetResult<T, R> submitMessageForGet(
      T configMessage);

  /**
   * Submit a message for a specific command which returns a list result of operations in progress.
   * This supports the {@link ClusterManagementService#list(ClusterManagementOperation)} command.
   *
   * @param <A> operation of type {@link ClusterManagementOperation}
   * @return {@link ClusterManagementListResult}
   */
  <A extends ClusterManagementOperation<V>, V extends OperationResult> ClusterManagementListOperationsResult<A, V> submitMessageForListOperation(
      A opType);

  /**
   * Submit a message for a specific command which returns a single operation in progress.
   * This supports the {@link ClusterManagementService#get(AbstractConfiguration)} command.
   *
   * @param <A> operation of type {@link ClusterManagementOperation}
   * @param operationId the identifier of the operation
   * @return {@link ClusterManagementListResult}
   */
  <A extends ClusterManagementOperation<V>, V extends OperationResult> ClusterManagementOperationResult<A, V> submitMessageForGetOperation(
      A opType, String operationId);

  /**
   * Submit a message to start a specific command. This supports the
   * {@link ClusterManagementService#start(ClusterManagementOperation)} command.
   *
   * @param <A> operation of type {@link ClusterManagementOperation}
   * @return {@link ClusterManagementListResult}
   */
  <A extends ClusterManagementOperation<V>, V extends OperationResult> ClusterManagementOperationResult<A, V> submitMessageForStart(
      A op);

  /**
   * Indicate whether this transport is currently connected
   *
   * @return boolean indicating whether connected
   */
  boolean isConnected();

  /**
   * Configure the transport with using connectionConfig.
   *
   * @param connectionConfig {@link ConnectionConfig} holding connection configuration information.
   */
  void configureConnection(ConnectionConfig connectionConfig);

  /**
   * Close the transport.
   */
  void close();
}
