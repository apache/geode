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

package org.apache.geode.cache.query;


/**
 * Thrown when an attribute or method name could not be resolved during query
 * execution because no matching method or field could be found.
 *
 * @since GemFire 4.0
 */

public class NameNotFoundException extends NameResolutionException {
private static final long serialVersionUID = 4827972941932684358L;
  /**
   * Constructs instance of ObjectNameNotFoundException with error message
   * @param message the error message
   */
  public NameNotFoundException(String message) {
    super(message);
  }
  
  /**
   * Constructs instance of ObjectNameNotFoundException with error message and cause
   * @param message the error message
   * @param cause a Throwable that is a cause of this exception
   */
  public NameNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
  
}
