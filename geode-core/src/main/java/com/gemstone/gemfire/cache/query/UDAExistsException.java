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
 * This Exception is thrown if a UDA with a given name already exists in the system,
 * and a new UDA is being registered with the same name
 * 
 * @author ashahid
 *
 */
public class UDAExistsException extends QueryException{

  private static final long serialVersionUID = -1643528839352144L;  
  
  public UDAExistsException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs instance of UDAExistsException with error message and cause
   * @param msg the error message
   * @param cause a Throwable that is a cause of this exception
   */
  public UDAExistsException(String msg, Throwable cause) {
    super(msg, cause);
  }
}