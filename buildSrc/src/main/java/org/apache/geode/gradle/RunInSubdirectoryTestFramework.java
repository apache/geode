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

package org.apache.geode.gradle;

import static java.nio.file.StandardCopyOption.COPY_ATTRIBUTES;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

import org.gradle.api.Action;
import org.gradle.api.internal.tasks.testing.TestFramework;
import org.gradle.api.internal.tasks.testing.WorkerTestClassProcessorFactory;
import org.gradle.api.internal.tasks.testing.detection.TestFrameworkDetector;
import org.gradle.api.tasks.testing.TestFrameworkOptions;
import org.gradle.process.internal.JavaExecHandleBuilder;
import org.gradle.process.internal.worker.WorkerProcessBuilder;

/**
 * Wraps a test framework to run each test worker in a separate working directory.
 */
public class RunInSubdirectoryTestFramework implements TestFramework {
  private static final String GEMFIRE_PROPERTIES = "gemfire.properties";
  private final AtomicLong workerId = new AtomicLong();
  private final TestFramework delegate;

  public RunInSubdirectoryTestFramework(TestFramework delegate) {
    this.delegate = delegate;
  }

  @Override
  public TestFrameworkDetector getDetector() {
    return delegate.getDetector();
  }

  @Override
  public TestFrameworkOptions getOptions() {
    return delegate.getOptions();
  }

  @Override
  public WorkerTestClassProcessorFactory getProcessorFactory() {
    return delegate.getProcessorFactory();
  }

  /**
   * Return an action that configures the test worker builder to run the test worker in a unique
   * subdirectory of the task's working directory.
   */
  @Override
  public Action<WorkerProcessBuilder> getWorkerConfigurationAction() {
    return workerProcessBuilder -> {
      delegate.getWorkerConfigurationAction().execute(workerProcessBuilder);
      JavaExecHandleBuilder javaCommand = workerProcessBuilder.getJavaCommand();

      Path taskWorkingDir = javaCommand.getWorkingDir().toPath();
      String workerWorkingDirName = String.format("test-worker-%06d", workerId.incrementAndGet());
      Path workerWorkingDir = taskWorkingDir.resolve(workerWorkingDirName);

      createWorkingDir(workerWorkingDir);
      copyGemFirePropertiesFile(taskWorkingDir, workerWorkingDir);

      javaCommand.setWorkingDir(workerWorkingDir);
    };
  }

  private void copyGemFirePropertiesFile(Path taskWorkingDir, Path workerWorkingDir) {
    Path taskPropertiesFile = taskWorkingDir.resolve(GEMFIRE_PROPERTIES);
    if (!Files.exists(taskPropertiesFile)) {
      return;
    }
    Path workerPropertiesFile = workerWorkingDir.resolve(taskPropertiesFile.getFileName());
    try {
      Files.copy(taskPropertiesFile, workerPropertiesFile, COPY_ATTRIBUTES);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void createWorkingDir(Path workerWorkingDir) {
    try {
      Files.createDirectories(workerWorkingDir);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
