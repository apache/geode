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

package org.apache.geode.gradle;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.gradle.api.Action;
import org.gradle.api.internal.DocumentationRegistry;
import org.gradle.api.internal.classpath.ModuleRegistry;
import org.gradle.api.internal.tasks.testing.TestClassProcessor;
import org.gradle.api.internal.tasks.testing.TestClassRunInfo;
import org.gradle.api.internal.tasks.testing.TestResultProcessor;
import org.gradle.api.internal.tasks.testing.WorkerTestClassProcessorFactory;
import org.gradle.api.internal.tasks.testing.worker.ForkingTestClassProcessor;
import org.gradle.api.internal.tasks.testing.worker.RemoteTestClassProcessor;
import org.gradle.api.internal.tasks.testing.worker.TestEventSerializer;
import org.gradle.api.internal.tasks.testing.worker.TestWorker;
import org.gradle.internal.remote.ObjectConnection;
import org.gradle.internal.work.WorkerLeaseRegistry;
import org.gradle.process.JavaForkOptions;
import org.gradle.process.internal.ExecException;
import org.gradle.process.internal.worker.WorkerProcess;
import org.gradle.process.internal.worker.WorkerProcessBuilder;
import org.gradle.process.internal.worker.WorkerProcessFactory;
import org.gradle.util.CollectionUtils;

/**
 * A copy of Gradle's {@link ForkingTestClassProcessor}, modified to establish a distinct test
 * context for each concurrently-running test JVM.
 * <p>
 * The class maintains a list of distinct, available test contexts. The list is created just before
 * the first processor launches a JVM, and initially includes enough contexts to supply the maximum
 * possible number of concurrent test JVMs. Before it launches a JVM, each processor claims a test
 * context and uses it to configure the Java command that launches the JVM. After a processor stops
 * a JVM, it releases its test context, making it available for assignment to future JVMs.
 */
public class IsolatingForkingTestClassProcessor implements TestClassProcessor {
  /**
   * The test contexts that are not claimed by a currently executing test class processor.
   */
  private static final AtomicReference<BlockingQueue<ParallelTestContext>> AVAILABLE_TEST_CONTEXTS =
      new AtomicReference<>();

  private final WorkerLeaseRegistry.WorkerLease currentWorkerLease;
  private final WorkerProcessFactory workerFactory;
  private final WorkerTestClassProcessorFactory processorFactory;
  private final JavaForkOptions options;
  private final Iterable<File> classPath;
  private final Action<WorkerProcessBuilder> buildConfigAction;
  private final ModuleRegistry moduleRegistry;
  private final Lock lock = new ReentrantLock();
  private final DocumentationRegistry documentationRegistry;
  private RemoteTestClassProcessor remoteProcessor;
  private WorkerProcess workerProcess;
  private TestResultProcessor resultProcessor;
  private WorkerLeaseRegistry.WorkerLeaseCompletion completion;
  private boolean stoppedNow;
  private ParallelTestContext testContext;

  public IsolatingForkingTestClassProcessor(WorkerLeaseRegistry.WorkerLease parentWorkerLease,
      WorkerProcessFactory workerFactory, WorkerTestClassProcessorFactory processorFactory,
      JavaForkOptions options, Iterable<File> classPath,
      Action<WorkerProcessBuilder> buildConfigAction, ModuleRegistry moduleRegistry,
      DocumentationRegistry documentationRegistry, int maxWorkerCount) {
    currentWorkerLease = parentWorkerLease;
    this.workerFactory = workerFactory;
    this.processorFactory = processorFactory;
    this.options = options;
    this.classPath = classPath;
    this.buildConfigAction = buildConfigAction;
    this.moduleRegistry = moduleRegistry;
    this.documentationRegistry = documentationRegistry;
    initializeTestContexts(maxWorkerCount);
  }

  @Override
  public void startProcessing(TestResultProcessor resultProcessor) {
    this.resultProcessor = resultProcessor;
  }

  @Override
  public void processTestClass(TestClassRunInfo testClass) {
    lock.lock();

    try {
      if (!stoppedNow) {
        if (remoteProcessor == null) {
          // To proceed, we need both a worker lease and a test context. Claim the test context
          // first. Otherwise we might unnecessarily hold a lease that we are not prepared to use,
          // which might block other tasks that need only a lease and not a test context.
          claimTestContext();
          completion = currentWorkerLease.startChild();
          try {
            remoteProcessor = forkProcess();
          } catch (RuntimeException e) {
            completion.leaseFinish();
            completion = null;
            releaseTestContext();
            throw e;
          }
        }

        remoteProcessor.processTestClass(testClass);
      }
    } finally {
      lock.unlock();
    }
  }

  RemoteTestClassProcessor forkProcess() {
    WorkerProcessBuilder builder = workerFactory.create(new TestWorker(processorFactory));
    builder.setBaseName("Gradle Test Executor");
    builder.setImplementationClasspath(getTestWorkerImplementationClasspath());
    builder.applicationClasspath(classPath);
    options.copyTo(builder.getJavaCommand());
    builder.getJavaCommand().jvmArgs("-Dorg.gradle.native=false");
    buildConfigAction.execute(builder);
    testContext.configure(builder.getJavaCommand());
    workerProcess = builder.build();
    workerProcess.start();
    ObjectConnection connection = workerProcess.getConnection();
    connection.useParameterSerializers(TestEventSerializer.create());
    connection.addIncoming(TestResultProcessor.class, resultProcessor);
    RemoteTestClassProcessor remoteProcessor =
        connection.addOutgoing(RemoteTestClassProcessor.class);
    connection.connect();
    remoteProcessor.startProcessing();
    return remoteProcessor;
  }

  List<URL> getTestWorkerImplementationClasspath() {
    return CollectionUtils.flattenCollections(URL.class,
        moduleRegistry.getModule("gradle-core-api").getImplementationClasspath().getAsURLs(),
        moduleRegistry.getModule("gradle-core").getImplementationClasspath().getAsURLs(),
        moduleRegistry.getModule("gradle-logging").getImplementationClasspath().getAsURLs(),
        moduleRegistry.getModule("gradle-messaging").getImplementationClasspath().getAsURLs(),
        moduleRegistry.getModule("gradle-base-services").getImplementationClasspath().getAsURLs(),
        moduleRegistry.getModule("gradle-cli").getImplementationClasspath().getAsURLs(),
        moduleRegistry.getModule("gradle-native").getImplementationClasspath().getAsURLs(),
        moduleRegistry.getModule("gradle-testing-base").getImplementationClasspath().getAsURLs(),
        moduleRegistry.getModule("gradle-testing-jvm").getImplementationClasspath().getAsURLs(),
        moduleRegistry.getModule("gradle-testing-junit-platform")
            .getImplementationClasspath().getAsURLs(),
        moduleRegistry.getExternalModule("junit-platform-engine")
            .getImplementationClasspath().getAsURLs(),
        moduleRegistry.getExternalModule("junit-platform-launcher")
            .getImplementationClasspath().getAsURLs(),
        moduleRegistry.getExternalModule("junit-platform-commons")
            .getImplementationClasspath().getAsURLs(),
        moduleRegistry.getModule("gradle-process-services")
            .getImplementationClasspath().getAsURLs(),
        moduleRegistry.getExternalModule("slf4j-api").getImplementationClasspath().getAsURLs(),
        moduleRegistry.getExternalModule("jul-to-slf4j").getImplementationClasspath().getAsURLs(),
        moduleRegistry.getExternalModule("native-platform")
            .getImplementationClasspath().getAsURLs(),
        moduleRegistry.getExternalModule("kryo").getImplementationClasspath().getAsURLs(),
        moduleRegistry.getExternalModule("commons-lang").getImplementationClasspath().getAsURLs(),
        moduleRegistry.getExternalModule("junit").getImplementationClasspath().getAsURLs());
  }

  @Override
  public void stop() {
    try {
      if (remoteProcessor != null) {
        lock.lock();

        try {
          if (!stoppedNow) {
            remoteProcessor.stop();
          }
        } finally {
          lock.unlock();
        }

        workerProcess.waitForStop();
      }
    } catch (ExecException e) {
      if (!stoppedNow) {
        throw new ExecException(e.getMessage()
            + "\nThis problem might be caused by incorrect test process configuration."
            + "\nPlease refer to the test execution section in the User Manual at "
            + documentationRegistry.getDocumentationFor("java_testing", "sec:test_execution"),
            e.getCause());
      }
    } finally {
      if (completion != null) {
        completion.leaseFinish();
      }
      releaseTestContext();
    }
  }

  @Override
  public void stopNow() {
    lock.lock();

    try {
      stoppedNow = true;
      if (remoteProcessor != null) {
        workerProcess.stopNow();
      }
    } finally {
      lock.unlock();
    }
  }

  private void claimTestContext() {
    BlockingQueue<ParallelTestContext> contexts = AVAILABLE_TEST_CONTEXTS.get();
    try {
      testContext = contexts.take();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void releaseTestContext() {
    if (testContext == null) {
      return;
    }
    BlockingQueue<ParallelTestContext> contexts = AVAILABLE_TEST_CONTEXTS.get();
    try {
      contexts.put(testContext);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    testContext = null;
  }

  private static void initializeTestContexts(int numberOfContexts) {
    AVAILABLE_TEST_CONTEXTS.updateAndGet(currentValue -> {
      if (currentValue != null) {
        return currentValue;
      }
      return new LinkedBlockingDeque<>(ParallelTestContext.create(numberOfContexts));
    });
  }
}
