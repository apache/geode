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

package org.apache.geode.gradle.testing.process;

import java.io.File;

import org.gradle.api.Action;
import org.gradle.api.internal.ClassPathRegistry;
import org.gradle.api.internal.file.TemporaryFileProvider;
import org.gradle.api.logging.LoggingManager;
import org.gradle.internal.id.IdGenerator;
import org.gradle.internal.jvm.inspection.JvmVersionDetector;
import org.gradle.internal.logging.events.OutputEventListener;
import org.gradle.internal.remote.MessagingServer;
import org.gradle.process.internal.JavaExecHandleFactory;
import org.gradle.process.internal.health.memory.MemoryManager;
import org.gradle.process.internal.worker.DefaultWorkerProcessFactory;
import org.gradle.process.internal.worker.WorkerProcessBuilder;
import org.gradle.process.internal.worker.WorkerProcessContext;

/**
 * Overrides Gradle's {@link DefaultWorkerProcessFactory} to return an {@link WorkerProcessBuilder}
 * that uses the given {@link ProcessLauncher} to launch worker processes.
 */
public class LauncherProxyWorkerProcessFactory extends DefaultWorkerProcessFactory {
  private final ProcessLauncher processLauncher;

  public LauncherProxyWorkerProcessFactory(LoggingManager loggingManager,
      MessagingServer server, ClassPathRegistry classPathRegistry,
      IdGenerator<Long> idGenerator, File gradleUserHomeDir,
      TemporaryFileProvider temporaryFileProvider,
      JavaExecHandleFactory execHandleFactory,
      JvmVersionDetector jvmVersionDetector,
      OutputEventListener outputEventListener,
      MemoryManager memoryManager, ProcessLauncher processLauncher) {
    super(loggingManager, server, classPathRegistry, idGenerator, gradleUserHomeDir,
        temporaryFileProvider, execHandleFactory, jvmVersionDetector, outputEventListener,
        memoryManager);
    this.processLauncher = processLauncher;
  }

  @Override
  public WorkerProcessBuilder create(Action<? super WorkerProcessContext> workerAction) {
    return new LauncherProxyWorkerProcessBuilder(super.create(workerAction), processLauncher);
  }
}
