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

package org.apache.geode.management.cluster.client.internal;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.NotImplementedException;

import org.apache.geode.management.api.ClusterManagementException;
import org.apache.geode.management.api.ClusterManagementGetResult;
import org.apache.geode.management.api.ClusterManagementListOperationsResult;
import org.apache.geode.management.api.ClusterManagementListResult;
import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.api.ClusterManagementOperationResult;
import org.apache.geode.management.api.ClusterManagementRealizationResult;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.api.ClusterManagementServiceTransport;
import org.apache.geode.management.api.CommandType;
import org.apache.geode.management.configuration.AbstractConfiguration;
import org.apache.geode.management.runtime.OperationResult;
import org.apache.geode.management.runtime.RuntimeInfo;

/**
 * Implementation of {@link ClusterManagementService} interface which represents the cluster
 * management service as used by a Java client.
 * <p/>
 * In order to manipulate Geode components (Regions, etc.) clients can construct instances of {@link
 * AbstractConfiguration}s and call the corresponding
 * {@link ClientClusterManagementService#create(AbstractConfiguration)},
 * {@link ClientClusterManagementService#delete(AbstractConfiguration)} or
 * {@link ClientClusterManagementService#update(AbstractConfiguration)} method. The returned {@link
 * ClusterManagementResult} will contain all necessary information about the outcome of the call.
 * This will include the result of persisting the config as part of the cluster configuration as
 * well as creating the actual component in the cluster.
 * <p/>
 * All create calls are idempotent and will not return an error if the desired component already
 * exists.
 */
public class ClientClusterManagementService implements ClusterManagementService {
  // the restTemplate needs to have the context as the baseUrl, and request URI is the part after
  // the context (including /v1), it needs to be set up this way so that spring test
  // runner's injected RequestFactory can work
  private final ClusterManagementServiceTransport transport;

  public ClientClusterManagementService(ClusterManagementServiceTransport transport) {
    this.transport = transport;
  }

  @Override
  public <T extends AbstractConfiguration<?>> ClusterManagementRealizationResult create(T config) {
    ClusterManagementRealizationResult result =
        transport.submitMessage(config, config.getCreationCommandType());
    assertSuccessful(result);
    return result;
  }

  @Override
  public <T extends AbstractConfiguration<?>> ClusterManagementRealizationResult delete(
      T config) {
    ClusterManagementRealizationResult result = transport.submitMessage(config, CommandType.DELETE);
    assertSuccessful(result);
    return result;
  }

  @Override
  public <T extends AbstractConfiguration<?>> ClusterManagementRealizationResult update(
      T config) {
    throw new NotImplementedException("Not Implemented");
  }

  @Override
  public <T extends AbstractConfiguration<R>, R extends RuntimeInfo> ClusterManagementListResult<T, R> list(
      T config) {
    ClusterManagementListResult<T, R> result = transport.submitMessageForList(config);
    assertSuccessful(result);
    return result;
  }

  @Override
  public <T extends AbstractConfiguration<R>, R extends RuntimeInfo> ClusterManagementGetResult<T, R> get(
      T config) {
    ClusterManagementGetResult<T, R> result = transport.submitMessageForGet(config);
    assertSuccessful(result);
    return result;
  }

  @Override
  public <A extends ClusterManagementOperation<V>, V extends OperationResult> ClusterManagementOperationResult<A, V> start(
      A op) {
    ClusterManagementOperationResult<A, V> result = transport.submitMessageForStart(op);
    assertSuccessful(result);
    return result;
  }

  @Override
  public <A extends ClusterManagementOperation<V>, V extends OperationResult> ClusterManagementOperationResult<A, V> get(
      A opType, String opId) {
    ClusterManagementOperationResult<A, V> result =
        transport.submitMessageForGetOperation(opType, opId);
    assertSuccessful(result);
    return result;
  }

  @Override
  public <A extends ClusterManagementOperation<V>, V extends OperationResult> CompletableFuture<ClusterManagementOperationResult<A, V>> getFuture(
      A opType, String opId) {
    AtomicReference<CompletableFuture<ClusterManagementOperationResult<A, V>>> futureAtomicReference =
        new AtomicReference<>();
    futureAtomicReference.set(CompletableFuture.supplyAsync(() -> {
      while (futureAtomicReference.get() == null || !futureAtomicReference.get().isCancelled()) {
        ClusterManagementOperationResult<A, V> result = this.get(opType, opId);
        if (result.getOperationEnd() != null) {
          return result;
        }
        try {
          Thread.sleep(1000L);
        } catch (InterruptedException e) {
          throw new ClusterManagementException(result, e);
        }
      }
      return null;
    }));
    return futureAtomicReference.get();
  }

  @Override
  public <A extends ClusterManagementOperation<V>, V extends OperationResult> ClusterManagementListOperationsResult<A, V> list(
      A opType) {
    ClusterManagementListOperationsResult<A, V> result =
        transport.submitMessageForListOperation(opType);
    assertSuccessful(result);
    return result;
  }

  @Override
  public boolean isConnected() {
    return transport.isConnected();
  }

  @Override
  public void close() {
    transport.close();
  }

  private void assertSuccessful(ClusterManagementResult result) {
    if (result == null) {
      ClusterManagementResult somethingVeryBadHappened = new ClusterManagementResult(
          ClusterManagementResult.StatusCode.ERROR, "Unable to parse server response.");
      throw new ClusterManagementException(somethingVeryBadHappened);
    } else if (!result.isSuccessful()) {
      throw new ClusterManagementException(result);
    }
  }
}
