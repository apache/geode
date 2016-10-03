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

package org.apache.geode.cache;

import org.apache.geode.GemFireException;

/** A generic runtime exception that indicates
 * a cache error has occurred. All the other runtime cache exceptions are the
 * subclass of this class. This class is abstract so only subclasses can be
 * instantiated
 *
 *
 * @since GemFire 3.0
 */
public abstract class CacheRuntimeException extends GemFireException {
  public static final long serialVersionUID = 6750107573015376688L;

  /**
   * Creates a new instance of <code>CacheRuntimeException</code> without detail message.
   */
  public CacheRuntimeException() {
  }
  
  
  /**
   * Constructs an instance of <code>CacheRuntimeException</code> with the specified detail message.
   * @param msg the detail message
   */
  public CacheRuntimeException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs an instance of <code>CacheRuntimeException</code> with the specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public CacheRuntimeException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructs an instance of <code>CacheRuntimeException</code> with the specified cause.
   * @param cause the causal Throwable
   */
  public CacheRuntimeException(Throwable cause) {
    super(cause);
  }

  @Override
  public String toString() {
    String result = super.toString();
    Throwable cause = getCause();
    if (cause != null) {
      String causeStr = cause.toString();
      final String glue = ", caused by ";
      StringBuffer sb = new StringBuffer(result.length() + causeStr.length() + glue.length());
      sb.append(result)
        .append(glue)
        .append(causeStr);
      result = sb.toString();
    }
    return result;
  }
}
