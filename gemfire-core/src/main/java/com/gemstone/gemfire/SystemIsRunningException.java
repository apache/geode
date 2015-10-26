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
 * A <code>SystemIsRunningException</code> is thrown when an operation
 * is attempted that requires that the locator is stopped.
 * <p>
 * In some cases this exception may be thrown and the locator will
 * not be running. This will happen if the locator was not stopped
 * cleanly.
 * <p>As of GemFire 5.0 this exception should be named LocatorIsRunningException.
 */
public class SystemIsRunningException extends GemFireException {
private static final long serialVersionUID = 3516268055878767189L;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>SystemIsRunningException</code>.
   */
  public SystemIsRunningException() {
    super();
  }

  /**
   * Creates a new <code>SystemIsRunningException</code>.
   */
  public SystemIsRunningException(String message) {
    super(message);
  }
}
