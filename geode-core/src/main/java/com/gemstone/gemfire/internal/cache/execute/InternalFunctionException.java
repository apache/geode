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
package com.gemstone.gemfire.internal.cache.execute;

import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionService;

/**
 * This is an exception used internally when the function sends the exception.
 * Exception sent through ResultSender.sendException will be wrapped
 * internally in InternalFunctionException. This InternalFunctionException will
 * be used to decide whether the exception should be added as a part of
 * addResult or exception should be thrown while doing ResultCollector#getResult
 * 
 * <p>
 * The exception string provides details on the cause of failure.
 * </p>
 * 
 * 
 * @since GemFire 6.6
 * @see FunctionService
 */

public class InternalFunctionException extends FunctionException {

  /**
   * Creates new internal function exception with given error message.
   * 
   */
  public InternalFunctionException() {
  }

  /**
   * Creates new internal function exception with given error message.
   * 
   * @param msg
   */
  public InternalFunctionException(String msg) {
    super(msg);
  }

  /**
   * Creates new internal function exception with given error message and optional nested
   * exception.
   * 
   * @param msg
   * @param cause
   */
  public InternalFunctionException(String msg, Throwable cause) {
    super(msg, cause);
  }

  /**
   * Creates new internal function exception given throwable as a cause and source of
   * error message.
   * 
   * @param cause
   */
  public InternalFunctionException(Throwable cause) {
    super(cause);
  }
}
