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
/**
 * 
 */
package com.gemstone.gemfire.pdx;

import com.gemstone.gemfire.GemFireException;

/**
 * Thrown if the PDX system could not be successfully initialized.
 * The cause will give the detailed reason why initialization failed.
 * @since GemFire 6.6
 *
 */
public class PdxInitializationException extends GemFireException {

  private static final long serialVersionUID = 5098737377658808834L;

  /**
   * Construct a new exception with the given message
   * @param message the message of the new exception
   */
  public PdxInitializationException(String message) {
    super(message);
  }

  /**
   * Construct a new exception with the given message and cause
   * @param message the message of the new exception
   * @param cause the cause of the new exception
   */
  public PdxInitializationException(String message, Throwable cause) {
    super(message, cause);
  }
}
