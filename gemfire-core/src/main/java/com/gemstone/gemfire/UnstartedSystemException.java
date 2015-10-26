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
package com.gemstone.gemfire;

/**
 * An <code>UnstartedSystemException</code> is thrown when the specified
 * locator exists but is not running or could not be connected to.
 * <p>
 * The most likely reasons for this are:
 * <ul>
 * <li> The locator has not completely started.
 * <li> The locator is stopping.
 * <li> The locator died or was killed.
 * </ul>
 * <p>As of GemFire 5.0 this exception should be named UnstartedLocatorException.
 */
public class UnstartedSystemException extends NoSystemException {
private static final long serialVersionUID = -4285897556527521788L;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>UnstartedSystemException</code>.
   */
  public UnstartedSystemException(String message) {
    super(message);
  }
  /**
   * Creates a new <code>UnstartedSystemException</code> with the given message
   * and cause.
   */
  public UnstartedSystemException(String message, Throwable cause) {
      super(message, cause);
  }
}
