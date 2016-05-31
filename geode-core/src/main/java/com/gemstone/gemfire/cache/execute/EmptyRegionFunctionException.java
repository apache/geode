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

import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * Exception to indicate that Region is empty for data aware functions.
 * 
 * @since GemFire 6.5
 * 
 */
public class EmptyRegionFunctionException extends FunctionException {

  private static final long serialVersionUID = 1L;

  /**
   * Construct an instance of EmtpyRegionFunctionException
   * 
   * @param cause
   *                a Throwable cause of this exception
   */
  public EmptyRegionFunctionException(Throwable cause) {
    super(cause);
  }

  /**
   * Construct an instance of EmtpyRegionFunctionException
   * 
   * @param msg
   *                Exception message
   */
  public EmptyRegionFunctionException(String msg) {
    super(msg);
  }

  /**
   * Construct an instance of EmtpyRegionFunctionException
   * 
   * @param msg
   *                the error message
   * @param cause
   *                a Throwable cause of this exception
   */
  public EmptyRegionFunctionException(String msg, Throwable cause) {
    super(msg, cause);
  }

}
