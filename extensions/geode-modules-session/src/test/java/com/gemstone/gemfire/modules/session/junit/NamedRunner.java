/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.gemstone.gemfire.modules.session.junit;

import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

/**
 * A Junit 4 runner that stores the name of the method that runs.  The methods
 * marked with @After and @Before (but also the Test method itself) can request
 * this name for which it is running.
 *
 * @author Rudy De Busscher
 */
public class NamedRunner extends BlockJUnit4ClassRunner {

  /**
   * The Constant PREFIX_KEY.
   */
  private static final String PREFIX_KEY = "ClassLoaderRunner_TestMethodName_";

  /**
   * The Constant NO_NAME.
   */
  private static final String NO_NAME = "null";

  /**
   * Instantiates a new named runner.
   *
   * @param klass the klass
   * @throws InitializationError the initialization error
   */
  public NamedRunner(Class<?> klass) throws InitializationError {
    super(klass);
  }

  @Override
  protected void runChild(FrameworkMethod method, RunNotifier notifier)

  {
    storeTestMethodName(method.getName());
    super.runChild(method, notifier);
    removeTestMethodName();
  }

  /**
   * Gets the test method name.
   *
   * @return the test method name
   */
  public static String getTestMethodName() {
    return retrieveTestMethodName();
  }

  /**
   * Retrieve test method name.
   *
   * @return the string
   */
  private static String retrieveTestMethodName() {
    // We can't use a ThreadLocal variable in the case the TestPerClassLoader runner is used.  Then this
    // Method is accessed from another classloader and thus reinitialized variables.
    String result = null;
    String storedName = System.getProperty(getKey());
    if (!NO_NAME.equals(storedName)) {
      result = storedName;
    }
    return result;
  }

  /**
   * Removes the test method name.
   */
  private static void removeTestMethodName() {
    // We can't use a ThreadLocal variable in the case the TestPerClassLoader runner is used.  Then this
    // Method is accessed from another classloader and thus reinitialized variables.
    System.setProperty(getKey(), NO_NAME);

  }

  /**
   * Store test method name.
   *
   * @param name the name
   */
  private static void storeTestMethodName(String name) {

    // We can't use a ThreadLocal variable in the case the TestPerClassLoader runner is used.  Then this
    // Method is accessed from another classloader and thus reinitialized variables.
    System.setProperty(getKey(), name);
  }

  /**
   * Gets the key.
   *
   * @return the key
   */
  private static String getKey() {
    StringBuffer buffer = new StringBuffer();
    buffer.append(PREFIX_KEY).append(Thread.currentThread().getName());
    return buffer.toString();
  }
}
