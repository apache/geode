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
 * Thrown if an attribute or method name in a query can be resolved to
 * more than one object in scope or if there is more than one maximally specific
 * overridden method in a class.
 *
 * @since GemFire 4.0
 */

public class AmbiguousNameException extends NameResolutionException {
private static final long serialVersionUID = 5635771575414148564L;
  
  /**
   * Constructs instance of AmbiguousNameException with error message
   * @param msg the error message
   */
  public AmbiguousNameException(String msg) {
    super(msg);
  }
    
  /**
   * Constructs instance of AmbiguousNameException with error message and cause
   * @param msg the error message
   * @param cause a Throwable that is a cause of this exception
   */
  public AmbiguousNameException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
