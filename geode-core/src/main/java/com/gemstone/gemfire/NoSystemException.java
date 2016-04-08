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
 * A <code>NoSystemException</code> is thrown when a
 * locator can not be found or connected to.
 * In most cases one of the following subclasses is used instead
 * of <code>NoSystemException</code>:
 * <ul>
 * <li> {@link UncreatedSystemException}
 * <li> {@link UnstartedSystemException}
 * </ul>
 * <p>As of GemFire 5.0 this exception should be named NoLocatorException.
 */
public class NoSystemException extends GemFireException {
private static final long serialVersionUID = -101890149467219630L;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>NoSystemException</code>.
   */
  public NoSystemException(String message) {
    super(message);
  }
  /**
   * Creates a new <code>NoSystemException</code> with the given message
   * and cause.
   */
  public NoSystemException(String message, Throwable cause) {
      super(message, cause);
  }
}
