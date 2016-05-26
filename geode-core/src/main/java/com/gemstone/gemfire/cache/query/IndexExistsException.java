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
/*
 * IndexNameConflictException.java
 *
 * Created on February 15, 2005, 10:20 AM
 */

package com.gemstone.gemfire.cache.query;

/**
 * Thrown while creating the new index if there exists an Index with
 * the same definition as new index.
 *
 * @since GemFire 4.0
 */

public class IndexExistsException extends QueryException{
private static final long serialVersionUID = -168312863985932144L;
  
  /**
   * Constructs instance of IndexNameConflictException with error message
   * @param msg the error message
   */
  public IndexExistsException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs instance of IndexNameConflictException with error message and cause
   * @param msg the error message
   * @param cause a Throwable that is a cause of this exception
   */
  public IndexExistsException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
