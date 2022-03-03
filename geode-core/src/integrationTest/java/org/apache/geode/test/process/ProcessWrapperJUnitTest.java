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

import org.junit.After;
import org.junit.Test;

import org.apache.geode.logging.internal.log4j.api.LogService;


/**
 * Integration tests for ProcessWrapper.
 *
 */
public class ProcessWrapperJUnitTest {

  private static final String OUTPUT_OF_MAIN = "Executing ProcessWrapperJUnitTest main";
  private ProcessWrapper process;

  @After
  public void after() {
    if (process != null) {
      process.destroy();
    }
  }

  @Test
  public void testClassPath() throws Exception {
    final String classPath = System.getProperty("java.class.path");
    assertTrue("Classpath is missing log4j-api: " + classPath,
        classPath.toLowerCase().contains("log4j-api"));
    assertTrue("Classpath is missing log4j-core: " + classPath,
        classPath.toLowerCase().contains("log4j-core"));
    assertTrue("Classpath is missing fastutil: " + classPath,
        classPath.toLowerCase().contains("fastutil"));

    process = new ProcessWrapper.Builder().mainClass(getClass()).build();
    process.execute();
    process.waitFor();

    assertTrue("Output is wrong: " + process.getOutput(),
        process.getOutput().contains(OUTPUT_OF_MAIN));
  }

  @Test
  public void testInvokeWithNullArgs() throws Exception {
    process = new ProcessWrapper.Builder().mainClass(getClass()).build();
    process.execute();
    process.waitFor();
    assertTrue(process.getOutput().contains(OUTPUT_OF_MAIN));
  }

  public static void main(String... args) throws Exception {
    Class.forName(org.apache.logging.log4j.LogManager.class.getName());
    Class.forName(LogService.class.getName());
    System.out.println(OUTPUT_OF_MAIN);
  }
}
