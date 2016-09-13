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

package com.gemstone.gemfire.cache.query;

/**
 * Thrown when the number of bound paramters for a query does not match the
 * number of placeholders.
 *
 * @since GemFire 4.0
 */
public class ParameterCountInvalidException extends QueryException {
private static final long serialVersionUID = -3249156440150789428L;
  
  /**
   * Creates a new instance of QueryParameterCountInvalidException
   * @param message the error message
   */
  public ParameterCountInvalidException(String message) {
    super(message);
  }
  
}
