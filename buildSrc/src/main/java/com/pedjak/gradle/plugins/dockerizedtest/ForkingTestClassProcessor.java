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
import java.net.URL;
import java.util.List;

import org.gradle.api.Action;
import org.gradle.api.internal.classpath.ModuleRegistry;
import org.gradle.api.internal.tasks.testing.TestClassProcessor;
import org.gradle.api.internal.tasks.testing.TestClassRunInfo;
import org.gradle.api.internal.tasks.testing.TestResultProcessor;
import org.gradle.api.internal.tasks.testing.WorkerTestClassProcessorFactory;
import org.gradle.api.internal.tasks.testing.worker.RemoteTestClassProcessor;
import org.gradle.api.internal.tasks.testing.worker.TestEventSerializer;
import org.gradle.internal.remote.ObjectConnection;
import org.gradle.process.JavaForkOptions;
import org.gradle.process.internal.worker.WorkerProcess;
import org.gradle.process.internal.worker.WorkerProcessBuilder;
import org.gradle.process.internal.worker.WorkerProcessFactory;
import org.gradle.util.CollectionUtils;

/**
 * DHE:
 * - A modified copy of Gradle's ForkingTestClassProcessor.
 */
public class ForkingTestClassProcessor implements TestClassProcessor {
  private final WorkerProcessFactory workerFactory;
  private final WorkerTestClassProcessorFactory processorFactory;
  private final JavaForkOptions options;
  private final Iterable<File> classPath;
  private final Action<WorkerProcessBuilder> buildConfigAction;
  private final ModuleRegistry moduleRegistry;
  private RemoteTestClassProcessor remoteProcessor;
  private WorkerProcess workerProcess;
  private TestResultProcessor resultProcessor;

  public ForkingTestClassProcessor(WorkerProcessFactory workerFactory,
                                   WorkerTestClassProcessorFactory processorFactory,
                                   JavaForkOptions options, Iterable<File> classPath,
                                   Action<WorkerProcessBuilder> buildConfigAction,
                                   ModuleRegistry moduleRegistry) {
    this.workerFactory = workerFactory;
    this.processorFactory = processorFactory;
    this.options = options;
    this.classPath = classPath;
    this.buildConfigAction = buildConfigAction;
    this.moduleRegistry = moduleRegistry;
  }

  @Override
  public void startProcessing(TestResultProcessor resultProcessor) {
    this.resultProcessor = resultProcessor;
  }

  @Override
  public void processTestClass(TestClassRunInfo testClass) {
    int i = 0;
    RuntimeException exception = null;
    // DHE: Why the loop? Does Gradle's lease mechanism make this redundant?
    while (remoteProcessor == null && i < 10) {
      try {
        remoteProcessor = forkProcess();
        exception = null;
        break;
      } catch (RuntimeException e) {
        exception = e;
        i++;
      }
    }

      if (exception != null) {
          throw exception;
      }
    remoteProcessor.processTestClass(testClass);
  }

  // DHE: Differences from Gradle v5.5:
  // - Creates a (custom) ForciblyStoppableTestWorker instead of a standard TestWorker
  // - Does not manage the lease.
  RemoteTestClassProcessor forkProcess() {
    WorkerProcessBuilder
        builder =
        workerFactory.create(new ForciblyStoppableTestWorker(processorFactory));
    builder.setBaseName("Gradle Test Executor");
    builder.setImplementationClasspath(getTestWorkerImplementationClasspath());
    builder.applicationClasspath(classPath);
    options.copyTo(builder.getJavaCommand());
    buildConfigAction.execute(builder);

    workerProcess = builder.build();
    workerProcess.start();

    ObjectConnection connection = workerProcess.getConnection();
    connection.useParameterSerializers(TestEventSerializer.create());
    connection.addIncoming(TestResultProcessor.class, resultProcessor);
    RemoteTestClassProcessor
        remoteProcessor =
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
        moduleRegistry.getModule("gradle-process-services").getImplementationClasspath()
            .getAsURLs(),
        moduleRegistry.getExternalModule("slf4j-api").getImplementationClasspath().getAsURLs(),
        moduleRegistry.getExternalModule("jul-to-slf4j").getImplementationClasspath().getAsURLs(),
        moduleRegistry.getExternalModule("native-platform").getImplementationClasspath()
            .getAsURLs(),
        moduleRegistry.getExternalModule("kryo").getImplementationClasspath().getAsURLs(),
        moduleRegistry.getExternalModule("commons-lang").getImplementationClasspath().getAsURLs(),
        moduleRegistry.getExternalModule("junit").getImplementationClasspath().getAsURLs(),
        ForkingTestClassProcessor.class.getProtectionDomain().getCodeSource().getLocation()
    );
  }

  // DHE: Differences from Gradle v5.5
  // - This stop() does not manage the lease.
  @Override
  public void stop() {
    if (remoteProcessor != null) {
      try {
        remoteProcessor.stop();
        workerProcess.waitForStop();
      } finally {
        // do nothing
      }
    }
  }

  // DHE: Differences from Gradle v5.5
  // - This stop() does not stop the worker process.
  // Stopping the worker process may eliminate the need for ForciblyStoppableTestWorker.
  @Override
  public void stopNow() {
    stop(); // TODO need anything else ??
  }

}
