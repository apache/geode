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
package org.apache.geode.gradle.testing.repeat;

import java.io.File;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.gradle.api.file.FileTree;
import org.gradle.api.internal.DocumentationRegistry;
import org.gradle.api.internal.classpath.ModuleRegistry;
import org.gradle.api.internal.tasks.testing.JvmTestExecutionSpec;
import org.gradle.api.internal.tasks.testing.TestClassProcessor;
import org.gradle.api.internal.tasks.testing.TestExecuter;
import org.gradle.api.internal.tasks.testing.TestFramework;
import org.gradle.api.internal.tasks.testing.TestResultProcessor;
import org.gradle.api.internal.tasks.testing.WorkerTestClassProcessorFactory;
import org.gradle.api.internal.tasks.testing.detection.DefaultTestClassScanner;
import org.gradle.api.internal.tasks.testing.detection.DefaultTestExecuter;
import org.gradle.api.internal.tasks.testing.detection.TestFrameworkDetector;
import org.gradle.api.internal.tasks.testing.filter.DefaultTestFilter;
import org.gradle.api.internal.tasks.testing.processors.MaxNParallelTestClassProcessor;
import org.gradle.api.internal.tasks.testing.processors.PatternMatchTestClassProcessor;
import org.gradle.api.internal.tasks.testing.processors.RestartEveryNTestClassProcessor;
import org.gradle.api.internal.tasks.testing.processors.RunPreviousFailedFirstTestClassProcessor;
import org.gradle.api.internal.tasks.testing.processors.TestMainAction;
import org.gradle.api.internal.tasks.testing.worker.ForkingTestClassProcessor;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.internal.Factory;
import org.gradle.internal.actor.ActorFactory;
import org.gradle.internal.time.Clock;
import org.gradle.internal.work.WorkerLeaseRegistry;
import org.gradle.process.internal.worker.WorkerProcessFactory;

/**
 * A copy of {@link DefaultTestExecuter} from Gradle v6.8.3, modified to process each test class
 * as many times as it was submitted. This is required by our {@link RepeatTest} task, because:
 * <ul>
 *   <li>Geode's {@code RepeatTest} task operates by submitting each test class for processing
 *   multiple times.</li>
 *   <li>Gradle's {@code DefaultTestExecuter} includes a
 *   {@link RunPreviousFailedFirstTestClassProcessor}, which de-duplicates the submitted test
 *   classes, preventing the {@code RepeatTest} from repeating the tests.</li>
 * </ul>
 * <p>
 * This executer omits the {@code RunPreviousFailedFirstTestClassProcessor}, and so each test class
 * is processed as many times as {@code RepeatTest} submits it. See the comment in {@link #execute}.
 * <p>
 * This executer also assigns an ID to each execution of a test class, and appends that ID onto the
 * class name for that test instance. Gradle writes each test's output to a log associated with
 * the class name. Appending an execution ID onto the class name causes Gradle to write the output
 * from each instance to its own log.
 */
public class RepeatTestExecuter implements TestExecuter<JvmTestExecutionSpec> {
  private static final Logger LOGGER = Logging.getLogger(RepeatTestExecuter.class);

  private final WorkerProcessFactory workerFactory;
  private final ActorFactory actorFactory;
  private final ModuleRegistry moduleRegistry;
  private final WorkerLeaseRegistry workerLeaseRegistry;
  private final int maxWorkerCount;
  private final Clock clock;
  private final DocumentationRegistry documentationRegistry;
  private final DefaultTestFilter testFilter;
  private final int iterationCount;
  private TestClassProcessor processor;

  public RepeatTestExecuter(WorkerProcessFactory workerFactory, ActorFactory actorFactory,
      ModuleRegistry moduleRegistry, WorkerLeaseRegistry workerLeaseRegistry, int maxWorkerCount,
      Clock clock, DocumentationRegistry documentationRegistry, DefaultTestFilter testFilter,
      int iterationCount) {
    this.workerFactory = workerFactory;
    this.actorFactory = actorFactory;
    this.moduleRegistry = moduleRegistry;
    this.workerLeaseRegistry = workerLeaseRegistry;
    this.maxWorkerCount = maxWorkerCount;
    this.clock = clock;
    this.documentationRegistry = documentationRegistry;
    this.testFilter = testFilter;
    this.iterationCount = iterationCount;
  }

  @Override
  public void execute(final JvmTestExecutionSpec testExecutionSpec,
      TestResultProcessor testResultProcessor) {
    final TestFramework testFramework = testExecutionSpec.getTestFramework();
    final WorkerTestClassProcessorFactory testInstanceFactory = testFramework.getProcessorFactory();
    final WorkerLeaseRegistry.WorkerLease
        currentWorkerLease =
        workerLeaseRegistry.getCurrentWorkerLease();
    final Set<File> classpath = ImmutableSet.copyOf(testExecutionSpec.getClasspath());
    final Set<File> modulePath = ImmutableSet.copyOf(testExecutionSpec.getModulePath());
    final List<String>
        testWorkerImplementationModules =
        testFramework.getTestWorkerImplementationModules();
    final Factory<TestClassProcessor> forkingProcessorFactory = new Factory<TestClassProcessor>() {
      @Override
      public TestClassProcessor create() {
        TestClassProcessor forkingTestClassProcessor =
            new ForkingTestClassProcessor(currentWorkerLease, workerFactory, testInstanceFactory,
                testExecutionSpec.getJavaForkOptions(),
                classpath, modulePath, testWorkerImplementationModules,
                testFramework.getWorkerConfigurationAction(), moduleRegistry,
                documentationRegistry);
        // Wrap the forking processor to make it distinguish each instance of a test class
        return new ExecutionTrackingForkingTestClassProcessor(forkingTestClassProcessor,
            iterationCount);
      }
    };
    final Factory<TestClassProcessor>
        reforkingProcessorFactory =
        new Factory<TestClassProcessor>() {
          @Override
          public TestClassProcessor create() {
            return new RestartEveryNTestClassProcessor(forkingProcessorFactory,
                testExecutionSpec.getForkEvery());
          }
        };
    // Create the chain of test class processors, omitting the
    // RunPreviousFailedFirstTestClassProcessor that Gradle's DefaultTestExecuter creates.
    processor =
        new PatternMatchTestClassProcessor(testFilter,
            new MaxNParallelTestClassProcessor(getMaxParallelForks(testExecutionSpec),
                reforkingProcessorFactory, actorFactory));

    final FileTree testClassFiles = testExecutionSpec.getCandidateClassFiles();

    Runnable detector;
    if (testExecutionSpec.isScanForTestClasses() && testFramework.getDetector() != null) {
      TestFrameworkDetector testFrameworkDetector = testFramework.getDetector();
      testFrameworkDetector.setTestClasses(testExecutionSpec.getTestClassesDirs().getFiles());
      testFrameworkDetector.setTestClasspath(classpath);
      detector = new DefaultTestClassScanner(testClassFiles, testFrameworkDetector, processor);
    } else {
      detector = new DefaultTestClassScanner(testClassFiles, null, processor);
    }

    new TestMainAction(detector, processor, testResultProcessor, clock, testExecutionSpec.getPath(),
        "Gradle Test Run " + testExecutionSpec.getIdentityPath()).run();
  }

  @Override
  public void stopNow() {
    if (processor != null) {
      processor.stopNow();
    }
  }

  private int getMaxParallelForks(JvmTestExecutionSpec testExecutionSpec) {
    int maxParallelForks = testExecutionSpec.getMaxParallelForks();
    if (maxParallelForks > maxWorkerCount) {
      LOGGER.info("{}.maxParallelForks ({}) is larger than max-workers ({}), forcing it to {}",
          testExecutionSpec.getPath(), maxParallelForks, maxWorkerCount, maxWorkerCount);
      maxParallelForks = maxWorkerCount;
    }
    return maxParallelForks;
  }
}
