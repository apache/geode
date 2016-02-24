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

import com.gemstone.gemfire.GemFireIOException;

/**
 * An exception indicating that a serialization or deserialization failed.
 * @since 5.7
 */
public class SerializationException extends GemFireIOException {
private static final long serialVersionUID = 7783018024920098997L;
  /**
   * 
   * Create a new instance of SerializationException with a detail message
   * @param message the detail message
   */
  public SerializationException(String message) {
    super(message);
  }

  /**
   * Create a new instance of SerializationException with a detail message and cause
   * @param message the detail message
   * @param cause the cause
   */
  public SerializationException(String message, Throwable cause) {
    super(message, cause);
  }
}
