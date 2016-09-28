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
package com.gemstone.gemfire.cache.execute;

import org.apache.geode.distributed.DistributedMember;


/**
 * Thrown if one of the function execution nodes goes away or cache is closed.
 * Function needs to be re-executed if the
 * {@link FunctionException#getCause()} is FunctionInvocationTargetException.
 * 
 * @since GemFire 6.0
 * @deprecated please use the org.apache.geode version of this class
 * 
 */
public class FunctionInvocationTargetException extends FunctionException {

  private static final long serialVersionUID = 1L;
  
  private DistributedMember id;
  
  /**
   * Construct an instance of FunctionInvocationTargetException
   * 
   * @param cause
   *                a Throwable cause of this exception
   */
  public FunctionInvocationTargetException(Throwable cause) {
    super(cause);
  }

  /**
   * Construct an instance of FunctionInvocationTargetException
   * 
   * @param msg
   *                the error message
   * @param id
   *                the DistributedMember id of the source
   * @since GemFire 6.5
   * 
   */
  public FunctionInvocationTargetException(String msg, DistributedMember id) {
    super(msg);
    this.id = id;
  }

  /**
   * Construct an instance of FunctionInvocationTargetException
   * 
   * @param msg
   *                Exception message
   */
  public FunctionInvocationTargetException(String msg) {
    super(msg);
  }
  /**
   * Construct an instance of FunctionInvocationTargetException
   * 
   * @param msg
   *                the error message
   * @param cause
   *                a Throwable cause of this exception
   */
  public FunctionInvocationTargetException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Method to get the member id of the Exception
   * 
   * @return DistributedMember id
   * @since GemFire 6.5
   */
  public DistributedMember getMemberId() {
    return this.id;
  }

}
