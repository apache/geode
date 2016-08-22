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
 * This error is used by GemFire for internal purposes.
 * It does not indicate an error condition.
 * For this reason it is named "Rethrowable" instead of the standard "Error".
 * It was made an <code>Error</code> to make it easier for user code that typically would
 * catch <code>Exception</code> to not accidently catch this exception.
 * <p> Note: if user code catches this error (or its subclasses) then it <em>must</em>
 * be rethrown.
 * 
 * @since GemFire 5.7
 */
public class GemFireRethrowable extends Error {
  private static final long serialVersionUID = 8349791552668922571L;

  /**
   * Create a GemFireRethrowable.
   */
  public GemFireRethrowable() {
  }

  /**
   * Create a GemFireRethrowable with the specified message.
   * @param message
   */
  public GemFireRethrowable(String message) {
    super(message);
  }
}
