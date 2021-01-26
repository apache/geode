/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pedjak.gradle.plugins.dockerizedtest;

import java.io.File;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableSet;
import org.gradle.api.file.FileTree;
import org.gradle.api.internal.classpath.ModuleRegistry;
import org.gradle.api.internal.tasks.testing.JvmTestExecutionSpec;
import org.gradle.api.internal.tasks.testing.TestClassProcessor;
import org.gradle.api.internal.tasks.testing.TestFramework;
import org.gradle.api.internal.tasks.testing.TestResultProcessor;
import org.gradle.api.internal.tasks.testing.WorkerTestClassProcessorFactory;
import org.gradle.api.internal.tasks.testing.detection.DefaultTestClassScanner;
import org.gradle.api.internal.tasks.testing.detection.TestFrameworkDetector;
import org.gradle.api.internal.tasks.testing.processors.MaxNParallelTestClassProcessor;
import org.gradle.api.internal.tasks.testing.processors.RestartEveryNTestClassProcessor;
import org.gradle.api.internal.tasks.testing.processors.TestMainAction;
import org.gradle.internal.Factory;
import org.gradle.internal.actor.ActorFactory;
import org.gradle.internal.operations.BuildOperationExecutor;
import org.gradle.internal.time.Clock;
import org.gradle.process.internal.worker.WorkerProcessFactory;

/**
 * DHE: A modified copy of Gradle's DefaultTestExecuter.
 * Modifications:
 * - Does not manage the worker lease.
 * - Omits some processors from the processor chain.
 *
 */
public class TestExecuter
    implements org.gradle.api.internal.tasks.testing.TestExecuter<JvmTestExecutionSpec> {
  private final WorkerProcessFactory workerFactory;
  private final ActorFactory actorFactory;
  private final ModuleRegistry moduleRegistry;
  private final BuildOperationExecutor buildOperationExecutor;
  private final Clock clock;
  private TestClassProcessor processor;

  public TestExecuter(WorkerProcessFactory workerFactory, ActorFactory actorFactory,
                      ModuleRegistry moduleRegistry, BuildOperationExecutor buildOperationExecutor,
                      Clock clock) {
    this.workerFactory = workerFactory;
    this.actorFactory = actorFactory;
    this.moduleRegistry = moduleRegistry;
    this.buildOperationExecutor = buildOperationExecutor;
    this.clock = clock;
  }

  // DHE: Differences from Gradle v5.5
  // - Does not manage the worker lease.
  // - Omits the PatternMatchTestClassProcessor and RunPreviousFailedFirstTestClassProcessor
  //   from the processor chain.
  // - Does not honor max-workers.
  @Override
  public void execute(final JvmTestExecutionSpec testExecutionSpec,
                      TestResultProcessor testResultProcessor) {
    final TestFramework testFramework = testExecutionSpec.getTestFramework();
    final WorkerTestClassProcessorFactory testInstanceFactory = testFramework.getProcessorFactory();
    final Set<File> classpath = ImmutableSet.copyOf(testExecutionSpec.getClasspath());
    final Factory<TestClassProcessor> forkingProcessorFactory = new Factory<TestClassProcessor>() {
      @Override
      public TestClassProcessor create() {
        return new ForkingTestClassProcessor(workerFactory, testInstanceFactory,
            testExecutionSpec.getJavaForkOptions(),
            classpath, testFramework.getWorkerConfigurationAction(), moduleRegistry);
      }
    };
    Factory<TestClassProcessor> reforkingProcessorFactory = new Factory<TestClassProcessor>() {
      @Override
      public TestClassProcessor create() {
        return new RestartEveryNTestClassProcessor(forkingProcessorFactory,
            testExecutionSpec.getForkEvery());
      }
    };

    processor = new MaxNParallelTestClassProcessor(testExecutionSpec.getMaxParallelForks(),
        reforkingProcessorFactory, actorFactory);

    final FileTree testClassFiles = testExecutionSpec.getCandidateClassFiles();

    Runnable detector;
    if (testExecutionSpec.isScanForTestClasses()) {
      TestFrameworkDetector
          testFrameworkDetector =
          testExecutionSpec.getTestFramework().getDetector();
      testFrameworkDetector.setTestClasses(testExecutionSpec.getTestClassesDirs().getFiles());
      testFrameworkDetector.setTestClasspath(classpath);
      detector = new DefaultTestClassScanner(testClassFiles, testFrameworkDetector, processor);
    } else {
      detector = new DefaultTestClassScanner(testClassFiles, null, processor);
    }

    Object testTaskOperationId;

    try {
      testTaskOperationId = buildOperationExecutor.getCurrentOperation().getParentId();
    } catch (Exception e) {
      testTaskOperationId = UUID.randomUUID();
    }

    new TestMainAction(detector, processor, testResultProcessor, clock, testTaskOperationId,
        testExecutionSpec.getPath(), "Gradle Test Run " + testExecutionSpec.getIdentityPath())
        .run();
  }

  @Override
  public void stopNow() {
    if (processor != null) {
      processor.stopNow();
    }
  }
}
