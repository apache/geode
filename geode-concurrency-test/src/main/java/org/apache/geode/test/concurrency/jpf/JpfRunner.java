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
package org.apache.geode.test.concurrency.jpf;

import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

import gov.nasa.jpf.Config;
import gov.nasa.jpf.JPF;
import gov.nasa.jpf.JPFListener;
import gov.nasa.jpf.search.Search;
import gov.nasa.jpf.search.SearchListenerAdapter;
import gov.nasa.jpf.vm.Verify;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.simple.SimpleLoggerContextFactory;

import org.apache.geode.test.concurrency.ParallelExecutor;

/**
 * Run a concurrent test using Java PathFinder
 */
public class JpfRunner implements org.apache.geode.test.concurrency.Runner {

  @Override
  public List<Throwable> runTestMethod(Method child) {
    List<Throwable> failures = new ArrayList<>();
    Config config = JPF.createConfig(new String[] {});
    config.setTarget(TestMain.class.getName());
    config.setTargetArgs(new String[] {child.getDeclaringClass().getName(), child.getName()});
    config.setProperty("report.probe_interval", "5");
    config.setProperty("peer_packages+", "org.apache.geode.test.concurrency.jpf.peers");
    config.setProperty("classpath", getClasspath());
    JPF jpf = new JPF(config);
    try {
      jpf.run();
    } catch (Throwable e) {
      failures.add(new AssertionError("JPF had an internal error", e));
    }

    jpf.getSearchErrors().stream().forEach(error -> failures
        .add(new AssertionError("JPF found test failures: " + error.getDescription())));

    return failures;
  }

  private String getClasspath() {
    Collection<String> classpath = pathElements(System.getProperty("java.class.path"));
    Collection<String> bootClasspath = pathElements(System.getProperty("sun.boot.class.path"));
    classpath.removeAll(bootClasspath);
    return String.join(File.pathSeparator, classpath);
  }

  private Collection<String> pathElements(String path) {
    return new LinkedHashSet<String>(Arrays.asList(path.split(File.pathSeparator)));
  }

  public static class TestMain {
    public static void main(String[] args) throws Exception {
      String clazzName = args[0];
      String methodName = args[1];
      System.setProperty(LogManager.FACTORY_PROPERTY_NAME,
          SimpleLoggerContextFactory.class.getName());
      Class clazz = Class.forName(clazzName);
      Object instance = clazz.newInstance();
      Method method = clazz.getMethod(methodName, ParallelExecutor.class);
      ParallelExecutorImpl parallelExecutor = new ParallelExecutorImpl();
      method.invoke(instance, parallelExecutor);
    }
  }
}
