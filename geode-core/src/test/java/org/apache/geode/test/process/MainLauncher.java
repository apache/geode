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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Method;

/**
 * Provides a main which delegates to another main for testing after waiting for one input. The
 * purpose is to prevent race condition in which a process may send output before the reader has
 * started listening for that output.
 *
 */
public class MainLauncher {
  public static void main(String... args) throws Exception {
    assert args.length > 0;
    String innerMain = args[0];
    Class<?> clazz = Class.forName(innerMain);

    // System.out.println(MainLauncher.class.getSimpleName() + " waiting to start...");
    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
    bufferedReader.readLine();

    // System.out.println(MainLauncher.class.getSimpleName() + " delegating...");
    Object[] innerArgs = new String[args.length - 1];
    for (int i = 0; i < innerArgs.length; i++) {
      innerArgs[i] = args[i + 1];
    }
    Method mainMethod = clazz.getMethod("main", String[].class);
    mainMethod.invoke(null, new Object[] {innerArgs});
  }
}
