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


import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.management.internal.api.LocatorClusterManagementService;
import org.apache.geode.management.internal.operations.OperationExecutor;
import org.apache.geode.management.operation.OperationResult;


@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/management-servlet.xml"},
    loader = PlainLocatorContextLoader.class)
@WebAppConfiguration
public class OperationManagementRestIntegrationTest {

  @Autowired
  private WebApplicationContext webApplicationContext;

  // needs to be used together with any BaseLocatorContextLoader
  private LocatorWebContext context;

  @Before
  public void before() {
    context = new LocatorWebContext(webApplicationContext);

    OperationExecutor goodOperation = new OperationExecutor() {
      @Override
      public CompletableFuture<OperationResult> run(
          Map<String, String> arguments, OperationResult operationResult,
          Executor exe) {
        final CompletableFuture<OperationResult> future = new CompletableFuture<>();
        exe.execute(() -> {
          operationResult.setStartTime(System.currentTimeMillis());
          // Do stuff
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
          operationResult.setStartTime(System.currentTimeMillis());
          operationResult.setStatus(OperationResult.Status.FAILED);
          operationResult.setMessage("operation failed");
          future.completeExceptionally(new RuntimeException("operation failed"));
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
  public void submitOperation() throws Exception {
    context.perform(post("/v2/operations/good-operation"))
        .andDo(print())
        .andExpect(status().isAccepted())
        .andExpect(header().string("Location", "http://localhost/v2/operations/good-operation"));
  }

  @Test
  public void querySuccessfulOperation() throws Exception {
    context.perform(post("/v2/operations/good-operation"))
        .andDo(print())
        .andExpect(status().isAccepted())
        .andExpect(header().string("Location", "http://localhost/v2/operations/good-operation"));

    context.perform(get("/v2/operations/good-operation"))
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status", is("COMPLETED")))
        .andExpect(jsonPath("$.startTime", is(greaterThan(0L))))
        .andExpect(jsonPath("$.endTime", is(greaterThan(0L))));
  }

  @Test
  public void queryOperationWithException() throws Exception {
    context.perform(post("/v2/operations/bad-operation"))
        .andDo(print())
        .andExpect(status().isAccepted())
        .andExpect(header().string("Location", "http://localhost/v2/operations/bad-operation"));

    context.perform(get("/v2/operations/bad-operation"))
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status", is("FAILED")));
  }

  @Test
  public void queryNonExistentOperation() throws Exception {
    context.perform(get("/v2/operations/unknown"))
        .andDo(print())
        .andExpect(status().isNotFound());
  }

}
