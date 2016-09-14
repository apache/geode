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
package org.apache.geode;

/**
 * An <code>UncreatedSystemException</code> is thrown when the specified
 * locator's directory or configuration file can not be found.
 * <p>
 * The most likely reasons for this are:
 * <ul>
 * <li> The wrong locator directory was given.
 * <li> The locator was deleted or never created.
 * </ul>
 * <p>As of GemFire 5.0 this exception should be named UncreatedLocatorException.
 */
public class UncreatedSystemException extends NoSystemException {
private static final long serialVersionUID = 5424354567878425435L;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>UncreatedSystemException</code>.
   */
  public UncreatedSystemException(String message) {
    super(message);
  }
  /**
   * Creates a new <code>UncreatedSystemException</code> with the given message
   * and cause.
   */
  public UncreatedSystemException(String message, Throwable cause) {
      super(message, cause);
  }
}
