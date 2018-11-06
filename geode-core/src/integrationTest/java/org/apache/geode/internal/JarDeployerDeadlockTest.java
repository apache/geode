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
package org.apache.geode.internal;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.test.compiler.ClassBuilder;

public class JarDeployerDeadlockTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  private ClassBuilder classBuilder;

  @Before
  public void setup() throws Exception {
    File workingDir = temporaryFolder.newFolder();
    ClassPathLoader.setLatestToDefault(workingDir);
    classBuilder = new ClassBuilder();
  }

  @After
  public void tearDown() throws Exception {
    for (String functionName : FunctionService.getRegisteredFunctions().keySet()) {
      FunctionService.unregisterFunction(functionName);
    }

    ClassPathLoader.setLatestToDefault();
  }

  @Test
  public void testMultiThreadingDoesNotCauseDeadlock() throws Exception {
    // Add two JARs to the classpath
    byte[] jarBytes = this.classBuilder.createJarFromName("JarClassLoaderJUnitA");
    File jarFile = temporaryFolder.newFile();
    IOUtils.copy(new ByteArrayInputStream(jarBytes), new FileOutputStream(jarFile));
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnitA.jar", jarFile);

    jarBytes = this.classBuilder.createJarFromClassContent("com/jcljunit/JarClassLoaderJUnitB",
        "package com.jcljunit; public class JarClassLoaderJUnitB {}");
    File jarFile2 = temporaryFolder.newFile();
    IOUtils.copy(new ByteArrayInputStream(jarBytes), new FileOutputStream(jarFile2));
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnitB.jar", jarFile2);

    String[] classNames = new String[] {"JarClassLoaderJUnitA", "com.jcljunit.JarClassLoaderJUnitB",
        "NON-EXISTENT CLASS"};

    final int threadCount = 10;
    ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
    for (int i = 0; i < threadCount; i++) {
      executorService.submit(new ForNameExerciser(classNames));
    }

    executorService.shutdown();
    await().until(executorService::isTerminated);

    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    long[] threadIds = threadMXBean.findDeadlockedThreads();

    if (threadIds != null) {
      StringBuilder deadLockTrace = new StringBuilder();
      for (long threadId : threadIds) {
        ThreadInfo threadInfo = threadMXBean.getThreadInfo(threadId, 100);
        deadLockTrace.append(threadInfo.getThreadName()).append("\n");
        for (StackTraceElement stackTraceElem : threadInfo.getStackTrace()) {
          deadLockTrace.append("\t").append(stackTraceElem).append("\n");
        }
      }
      System.out.println(deadLockTrace);
    }
    assertThat(threadIds).isNull();
  }

  private class ForNameExerciser implements Runnable {
    private final Random random = new Random();

    private final int numLoops = 1000;
    private final String[] classNames;

    ForNameExerciser(final String[] classNames) {
      this.classNames = classNames;
    }

    @Override
    public void run() {
      for (int i = 0; i < this.numLoops; i++) {
        try {
          // Random select a name from the list of class names and try to load it
          String className = this.classNames[random.nextInt(this.classNames.length)];
          ClassPathLoader.getLatest().forName(className);
        } catch (ClassNotFoundException expected) { // expected
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

}
