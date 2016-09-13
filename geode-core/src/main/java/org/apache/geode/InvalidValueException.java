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
 * An <code>InvalidValueException</code> is thrown when an attempt is
 * made to set a configuration attribute to an invalid value is made.
 * Values are considered invalid when they are
 * not compatible with the attribute's type.
 */
public class InvalidValueException extends GemFireException {
private static final long serialVersionUID = 6186767885369527709L;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>InvalidValueException</code>.
   */
  public InvalidValueException(String message) {
    super(message);
  }
  /**
   * Creates a new <code>InvalidValueException</code>.
   */
  public InvalidValueException(String message, Throwable ex) {
    super(message, ex);
  }
}
