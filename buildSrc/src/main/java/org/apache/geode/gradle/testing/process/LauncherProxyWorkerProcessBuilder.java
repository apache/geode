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

import static org.apache.geode.gradle.testing.process.Reflection.getField;
import static org.apache.geode.gradle.testing.process.Reflection.getFieldValue;
import static org.apache.geode.gradle.testing.process.Reflection.setFieldValue;

import java.io.File;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.util.List;
import java.util.Set;

import org.gradle.api.Action;
import org.gradle.api.logging.LogLevel;
import org.gradle.process.internal.JavaExecHandleBuilder;
import org.gradle.process.internal.worker.WorkerProcess;
import org.gradle.process.internal.worker.WorkerProcessBuilder;
import org.gradle.process.internal.worker.WorkerProcessContext;

/**
 * Wraps a worker process builder to make it use the given process launcher.
 */
public class LauncherProxyWorkerProcessBuilder implements WorkerProcessBuilder {
  private final WorkerProcessBuilder delegate;
  private final ProcessLauncher processLauncher;
  private static Object processLauncherProxy;

  public LauncherProxyWorkerProcessBuilder(WorkerProcessBuilder delegate,
      ProcessLauncher processLauncher) {
    this.delegate = delegate;
    this.processLauncher = processLauncher;
  }

  @Override
  public WorkerProcessBuilder applicationClasspath(Iterable<File> files) {
    return delegate.applicationClasspath(files);
  }

  @Override
  public Set<File> getApplicationClasspath() {
    return delegate.getApplicationClasspath();
  }

  @Override
  public WorkerProcessBuilder applicationModulePath(Iterable<File> files) {
    return delegate.applicationModulePath(files);
  }

  @Override
  public Set<File> getApplicationModulePath() {
    return delegate.getApplicationModulePath();
  }

  @Override
  public WorkerProcessBuilder setBaseName(String baseName) {
    return delegate.setBaseName(baseName);
  }

  @Override
  public String getBaseName() {
    return delegate.getBaseName();
  }

  @Override
  public WorkerProcessBuilder setLogLevel(LogLevel logLevel) {
    return delegate.setLogLevel(logLevel);
  }

  @Override
  public WorkerProcessBuilder sharedPackages(Iterable<String> packages) {
    return delegate.sharedPackages(packages);
  }

  @Override
  public Set<String> getSharedPackages() {
    return delegate.getSharedPackages();
  }

  @Override
  public JavaExecHandleBuilder getJavaCommand() {
    return delegate.getJavaCommand();
  }

  @Override
  public LogLevel getLogLevel() {
    return delegate.getLogLevel();
  }

  @Override
  public WorkerProcessBuilder sharedPackages(String... packages) {
    return delegate.sharedPackages(packages);
  }

  @Override
  public Action<? super WorkerProcessContext> getWorker() {
    return delegate.getWorker();
  }

  @Override
  public void setImplementationClasspath(List<URL> implementationClasspath) {
    delegate.setImplementationClasspath(implementationClasspath);
  }

  @Override
  public void setImplementationModulePath(List<URL> implementationModulePath) {
    delegate.setImplementationModulePath(implementationModulePath);
  }

  @Override
  public void enableJvmMemoryInfoPublishing(boolean shouldPublish) {
    delegate.enableJvmMemoryInfoPublishing(shouldPublish);
  }

  /**
   * Replaces the standard worker process's process launcher with this builder's launcher.
   */
  @Override
  public WorkerProcess build() {
    WorkerProcess workerProcess = delegate.build();
    Object workerProcessDelegate = getFieldValue(workerProcess, "delegate");
    Object execHandle = getFieldValue(workerProcessDelegate, "execHandle");
    Class<?> processLauncherType = getField(execHandle, "processLauncher").getType();
    setFieldValue(execHandle, "processLauncher", assignableProcessLauncher(processLauncherType));
    return workerProcess;
  }

  /**
   * Because the exec handle created by Gradle uses a classloader different from ours, we can't
   * simply construct a Gradle {@code ProcessLauncher) to assign. Instead we create proxy, using the
   * exec handle's classloader.
   */
  private synchronized Object assignableProcessLauncher(Class<?> requiredType) {
    if (processLauncherProxy == null) {
      // Assume that only start() will be called, and simply delegate to this builder's launcher
      InvocationHandler handler =
          (proxy, method, args) -> processLauncher.start((ProcessBuilder) args[0]);
      ClassLoader classLoader = requiredType.getClassLoader();
      Class<?>[] interfaces = {requiredType};
      processLauncherProxy = Proxy.newProxyInstance(classLoader, interfaces, handler);
    }
    return processLauncherProxy;
  }
}
