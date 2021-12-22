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
package org.apache.geode.test.process;

import static org.junit.Assert.assertTrue;
import static org.junit.contrib.java.lang.system.TextFromStandardInputStream.emptyStandardInputStream;

import java.lang.reflect.Method;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.TextFromStandardInputStream;

import org.apache.geode.test.junit.rules.ExecutorServiceRule;

/**
 * Quick sanity tests to make sure MainLauncher is functional.
 */
public class MainLauncherJUnitTest {

  private static final long TIMEOUT_SECONDS = 10;

  private static volatile boolean flag = false;

  private final String launchedClass = getClass().getName();

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @Rule
  public TextFromStandardInputStream systemInMock = emptyStandardInputStream();

  @Before
  public void before() {
    flag = false;
  }

  @Test
  public void testInvokeMainWithNullArgs() throws Exception {
    Class<?> clazz = getClass();
    Method mainMethod = clazz.getMethod("main", String[].class);
    String[] args = null;
    mainMethod.invoke(null, new Object[] {args});
    assertTrue(flag);
  }

  @Test
  public void testInvokeMainWithEmptyArgs() throws Exception {
    Class<?> clazz = getClass();
    Method mainMethod = clazz.getMethod("main", String[].class);
    String[] args = new String[0];
    mainMethod.invoke(null, new Object[] {args});
    assertTrue(flag);
  }

  @Test
  public void testInvokeMainWithOneArg() throws Exception {
    Class<?> clazz = getClass();
    Method mainMethod = clazz.getMethod("main", String[].class);
    String[] args = new String[] {"arg0"};
    mainMethod.invoke(null, new Object[] {args});
    assertTrue(flag);
  }

  @Test
  public void testInvokeMainWithTwoArgs() throws Exception {
    Class<?> clazz = getClass();
    Method mainMethod = clazz.getMethod("main", String[].class);
    String[] args = new String[] {"arg0", "arg1"};
    mainMethod.invoke(null, new Object[] {args});
    assertTrue(flag);
  }

  @Test
  public void testInvokeMainWithMainLauncherWithNoArgs() throws Exception {
    Future<Boolean> future = executorServiceRule.submit(() -> {
      Class<?> clazz = MainLauncher.class;
      Method mainMethod = clazz.getMethod("main", String[].class);
      String[] args = new String[] {launchedClass};
      // this will block until "\n" is fed to System.in
      mainMethod.invoke(null, new Object[] {args});
      return true;
    });
    systemInMock.provideText("\n");
    assertTrue(future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertTrue(flag);
  }

  @Test
  public void testInvokeMainWithMainLauncherWithOneArg() throws Exception {
    Future<Boolean> future = executorServiceRule.submit(() -> {
      Class<?> clazz = MainLauncher.class;
      Method mainMethod = clazz.getMethod("main", String[].class);
      String[] args = new String[] {launchedClass, "arg0"};
      mainMethod.invoke(null, new Object[] {args});
      return true;
    });
    systemInMock.provideText("\n");
    assertTrue(future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertTrue(flag);
  }

  @Test
  public void testInvokeMainWithMainLauncherWithTwoArgs() throws Exception {
    Future<Boolean> future = executorServiceRule.submit(() -> {
      Class<?> clazz = MainLauncher.class;
      Method mainMethod = clazz.getMethod("main", String[].class);
      String[] args = new String[] {launchedClass, "arg0", "arg1"};
      mainMethod.invoke(null, new Object[] {args});
      return true;
    });
    systemInMock.provideText("\n");
    assertTrue(future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertTrue(flag);
  }

  public static void main(String... args) throws Exception {
    flag = true;
  }
}
