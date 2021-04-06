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
package org.apache.geode.gradle.testing.isolation;

import java.util.List;
import java.util.function.Consumer;

import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

import org.apache.geode.gradle.testing.process.AdjustableProcessLauncher;

public class PortRangeProcessLauncher extends AdjustableProcessLauncher {
  private static final Logger LOGGER = Logging.getLogger(PortRangeProcessLauncher.class);
  private static List<PortRangeContext> availableContexts;

  public PortRangeProcessLauncher(int maxWorkers, Consumer<ProcessBuilder> adjustment) {
    super(adjustment);
    initializeContexts(maxWorkers);
  }

  @Override
  public Process start(ProcessBuilder processBuilder) {
    List<String> command = processBuilder.command();
    String workerName = command.get(command.size() - 1);
    PortRangeContext context = acquireContext(workerName);
    try {
      context.configure(processBuilder);
      Process process = super.start(processBuilder);
      return new CompletableProcess(workerName, process, () -> releaseContext(workerName, context));
    } catch(Throwable e) {
      releaseContext(workerName, context);
      throw e;
    }
  }

  private static synchronized void initializeContexts(int numberOfContexts) {
    if (availableContexts == null) {
      availableContexts = PortRangeContext.create(numberOfContexts);
      LOGGER.debug("Initialized {} port range contexts: {}", numberOfContexts, availableContexts);
    }
  }

  private static synchronized PortRangeContext acquireContext(String owner) {
    PortRangeContext context = availableContexts.remove(0);
    LOGGER
      .debug("{} acquired {} ({} available contexts)", owner, context, availableContexts.size());
    return context;
  }

  private static synchronized void releaseContext(String owner, PortRangeContext context) {
    availableContexts.add(context);
    LOGGER
      .debug("{} released {} ({} available contexts)", owner, context, availableContexts.size());
  }
}
