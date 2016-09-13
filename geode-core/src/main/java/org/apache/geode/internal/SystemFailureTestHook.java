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
package org.apache.geode.internal;

/**
 * Allows tests to expect certain exceptions without the SystemFailure watchdog getting upset.
 * See bug 46988.
 * 
 * @since GemFire 7.0.1
 */
public class SystemFailureTestHook {

  private static Class<?> expectedClass;
  
  /**
   * If a test sets this to a non-null value then it should also
   * set it to "null" with a finally block.
   */
  public static void setExpectedFailureClass(Class<?> expected) {
    expectedClass = expected;
  }

  /**
   * Returns true if the given failure is expected.
   */
  public static boolean errorIsExpected(Error failure) {
    return expectedClass != null && expectedClass.isInstance(failure);
  }

  public static void loadEmergencyClasses() {
    // nothing more needed
  }
}
