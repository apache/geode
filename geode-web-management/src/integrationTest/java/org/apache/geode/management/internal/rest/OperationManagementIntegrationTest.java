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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.internal.api.LocatorClusterManagementService;
import org.apache.geode.management.internal.operations.OperationExecutor;
import org.apache.geode.management.operation.OperationResult;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/management-servlet.xml"},
    loader = PlainLocatorContextLoader.class)
@WebAppConfiguration
public class OperationManagementIntegrationTest {

  @Autowired
  private WebApplicationContext webApplicationContext;

  // needs to be used together with any BaseLocatorContextLoader
  private LocatorWebContext context;

  private ClusterManagementService client;

  @Before
  public void before() {
    context = new LocatorWebContext(webApplicationContext);
    client = ClusterManagementServiceBuilder.buildWithRequestFactory()
        .setRequestFactory(context.getRequestFactory()).build();

    OperationExecutor goodOperation = new OperationExecutor() {
      @Override
      public CompletableFuture<OperationResult> run(
          Map<String, String> arguments, OperationResult operationResult,
          Executor exe) {
        final CompletableFuture<OperationResult> future = new CompletableFuture<>();
        exe.execute(() -> {
          operationResult.setStartTime(System.currentTimeMillis());
          // Do stuff
          if (arguments != null) {
            operationResult.setMessage(arguments.toString());
          }
          operationResult.setEndTime(System.currentTimeMillis());
          operationResult.setStatus(OperationResult.Status.COMPLETED);
          future.complete(operationResult);
        });

        return future;
      }

      @Override
      public String getId() {
        return "good-operation";
      }
    };

    OperationExecutor badOperation = new OperationExecutor() {

      @Override
      public CompletableFuture<OperationResult> run(
          Map<String, String> arguments, OperationResult operationResult,
          Executor exe) {
        final CompletableFuture<OperationResult> future = new CompletableFuture<>();
        exe.execute(() -> {
          if (arguments != null) {
            operationResult.setMessage(arguments.toString());
          }
          operationResult.setStatus(OperationResult.Status.FAILED);
        });

        return future;
      }

      @Override
      public String getId() {
        return "bad-operation";
      }
    };

    ((LocatorClusterManagementService) context.getLocator().getClusterManagementService())
        .registerOperation(goodOperation);
    ((LocatorClusterManagementService) context.getLocator().getClusterManagementService())
        .registerOperation(badOperation);
  }

  @Test
  public void submitSuccessfulOperationWithoutArguments() throws Exception {
    CompletableFuture<OperationResult> future = client.perform("good-operation", null);
    OperationResult result = future.get(1, TimeUnit.SECONDS);

    assertThat(result.getStatus()).isEqualTo(OperationResult.Status.COMPLETED);
  }

  @Test
  public void submitSuccessfulOperationWithArguments() throws Exception {
    Map<String, String> args = new HashMap<>();
    args.put("key", "value");
    CompletableFuture<OperationResult> future = client.perform("good-operation", args);
    OperationResult result = future.get(1, TimeUnit.SECONDS);

    assertThat(result.getStatus()).isEqualTo(OperationResult.Status.COMPLETED);
    assertThat(result.getMessage()).isEqualTo("{key=value}");
  }

  @Test
  public void submitOperationWithError() throws Exception {
    CompletableFuture<OperationResult> future = client.perform("bad-operation", null);
    OperationResult result = future.get(1000, TimeUnit.SECONDS);

    assertThat(result.getStatus()).isEqualTo(OperationResult.Status.FAILED);
  }

  @Test
  public void submitUnknownOperation() throws Exception {
    CompletableFuture<OperationResult> result = client.perform("unknown-operation", null);

    assertThat(result.isCompletedExceptionally()).isTrue();
  }

}
