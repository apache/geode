/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.cache.query;

import org.apache.geode.GemFireException;

/**
 * Thrown if the query language syntax is not valid.
 *
 * @since GemFire 4.0
 */

public class QueryInvalidException extends GemFireException {
  private static final long serialVersionUID = 2849255122285215114L;

  /**
   * Construct an instance of QueryInvalidException
   * 
   * @param msg the error message
   */
  public QueryInvalidException(String msg) {
    super(msg);
  }

  /**
   * Construct an instance of QueryInvalidException
   * 
   * @param msg the error message
   * @param cause a Throwable cause
   */
  public QueryInvalidException(String msg, Throwable cause) {
    super(msg);
    initCause(cause);
  }
}
