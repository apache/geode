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

import org.apache.geode.GemFireCheckedException;

/**
 * Thrown during by the query engine during parsing or execution. Instances of subclasses are thrown
 * for more specific exceptions.
 * 
 * @since GemFire 4.0
 */


public /* abstract */ class QueryException extends GemFireCheckedException {
  private static final long serialVersionUID = 7100830250939955452L;

  /**
   * Required for serialization
   */
  public QueryException() {

  }

  /**
   * Constructor used by concrete subclasses
   * 
   * @param msg the error message
   * @param cause a Throwable cause of this exception
   */
  public QueryException(String msg, Throwable cause) {
    super(msg, cause);
  }

  /**
   * Constructor used by concrete subclasses
   * 
   * @param msg the error message
   */
  public QueryException(String msg) {
    super(msg);
  }

  /**
   * Constructor used by concrete subclasses
   * 
   * @param cause a Throwable cause of this exception
   */
  public QueryException(Throwable cause) {
    super(cause);
  }


}
