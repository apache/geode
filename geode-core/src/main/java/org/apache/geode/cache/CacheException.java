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


/**
 * A generic exception, which indicates
 * a cache error has occurred. All the other cache exceptions are 
 * subclasses of this class. This class is abstract and therefore only
 * subclasses are instantiated.
 *
 *
 * @since GemFire 2.0
 */
public abstract class CacheException extends GemFireException {
  public static final long serialVersionUID = 7699432887938858940L;
  
  /** Constructs a new <code>CacheException</code>. */
  public CacheException() {
    super();
  }
  
  /** Constructs a new <code>CacheException</code> with a message string. */
  public CacheException(String s) {
    super(s);
  }
  
  /** Constructs a <code>CacheException</code> with a message string and
   * a base exception
   */
  public CacheException(String s, Throwable cause) {
    super(s, cause);
  }
  
  /** Constructs a <code>CacheException</code> with a cause */
  public CacheException(Throwable cause) {
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
