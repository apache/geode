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
package org.apache.geode;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import org.junit.Test;

public class CompletableFutureTest {

  private final Collection<CompletableFuture<Void>> startupTasks = new ArrayList<>();

  private CompletableFuture<Void> serverStartup;

  @Test
  public void noTasks() {
    serverStartup = CompletableFuture.allOf(startupTasks.toArray(new CompletableFuture[0]));
    serverStartup.thenRunAsync(() -> System.out.println("Server is online"));

    await().until(() -> serverStartup.isDone());
  }

  @Test
  public void oneTask() {
    CompletableFuture<Void> asyncTask1 = new CompletableFuture<>();
    startupTasks.add(asyncTask1);

    serverStartup = CompletableFuture.allOf(startupTasks.toArray(new CompletableFuture[0]));
    serverStartup.thenRunAsync(() -> System.out.println("Server is online"));

    for (CompletableFuture<Void> asyncTask : startupTasks) {
      asyncTask.complete(null);
    }

    await().until(() -> serverStartup.isDone());
  }

  @Test
  public void manyTasks() {
    CompletableFuture<Void> asyncTask1 = new CompletableFuture<>();
    CompletableFuture<Void> asyncTask2 = new CompletableFuture<>();
    CompletableFuture<Void> asyncTask3 = new CompletableFuture<>();
    startupTasks.add(asyncTask1);
    startupTasks.add(asyncTask2);
    startupTasks.add(asyncTask3);

    serverStartup = CompletableFuture.allOf(startupTasks.toArray(new CompletableFuture[0]));
    serverStartup.thenRunAsync(() -> System.out.println("Server is online"));

    for (CompletableFuture<Void> asyncTask : startupTasks) {
      asyncTask.complete(null);
    }

    await().until(() -> serverStartup.isDone());
  }

  @Test
  public void justPlayingAroundWithCompletionOrdering() throws InterruptedException {
    CompletableFuture<Void> asyncTask1 = new CompletableFuture<>();
    CompletableFuture<Void> asyncTask2 = new CompletableFuture<>();
    CompletableFuture<Void> asyncTask3 = new CompletableFuture<>();
    startupTasks.add(asyncTask1);
    startupTasks.add(asyncTask2);
    startupTasks.add(asyncTask3);

    serverStartup = CompletableFuture.allOf(startupTasks.toArray(new CompletableFuture[0]));
    serverStartup.thenRunAsync(() -> System.out.println("Server is online"));

    asyncTask3.complete(null);
    Thread.sleep(2000);
    asyncTask2.complete(null);
    Thread.sleep(100);
    asyncTask1.complete(null);

    await().until(() -> serverStartup.isDone());
  }
}
