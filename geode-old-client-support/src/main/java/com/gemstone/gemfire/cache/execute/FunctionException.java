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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.gemstone.gemfire.GemFireException;
import org.apache.geode.internal.Assert;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;

/**
 * Thrown to indicate an error or exceptional condition during the execution of 
 * {@linkplain Function}s in GemFire. This exception can be thrown by GemFire 
 * as well as user code, in the implementation of {@linkplain Function#execute(FunctionContext)}.
 * When FunctionException is thrown in an implementation of 
 * {@linkplain Function#execute(FunctionContext)}, GemFire will transmit it back 
 * to, and throw it on, the calling side. For example, if a GemFire client 
 * executes a Function on a server, and the function's execute method throws a
 * FunctionException, the server logs the exception as a warning, and transmits
 * it back to the calling client, which throws it. This allows for separation of
 * business and error handling logic, as client code that processes function 
 * execution results does not have to deal with errors; errors can be dealt with
 * in the exception handling logic, by catching this exception.
 *
 * <p>The exception string provides details on the cause of failure.
 * </p>
 * 
 * 
 * @since GemFire 6.0
 * @see FunctionService
 * @deprecated please use the org.apache.geode version of this class
 */
public class FunctionException extends GemFireException {

  private static final long serialVersionUID = 4893171227542647452L;

  private transient ArrayList<Throwable> exceptions;

  /**
   * Creates new function exception with given error message.
   * 
   * @since GemFire 6.5
   */
  public FunctionException() {
  }

  /**
   * Creates new function exception with given error message.
   * 
   * @param msg
   * @since GemFire 6.0
   */
  public FunctionException(String msg) {
    super(msg);
  }

  /**
   * Creates new function exception with given error message and optional nested
   * exception.
   * 
   * @param msg
   * @param cause
   * @since GemFire 6.0
   */
  public FunctionException(String msg, Throwable cause) {
    super(msg, cause);
  }

  /**
   * Creates new function exception given throwable as a cause and source of
   * error message.
   * 
   * @param cause
   * @since GemFire 6.0
   */
  public FunctionException(Throwable cause) {
    super(cause);
  }

  /**
   * Adds exceptions thrown from different nodes to a ds
   * 
   * @param cause
   * @since GemFire 6.5
   */
  public final void addException(Throwable cause) {
    Assert.assertTrue(cause != null,
        "unexpected null exception to add to FunctionException");
    getExceptions().add(cause);
  }

  /**
   * Returns the list of exceptions thrown from different nodes
   * 
   * @since GemFire 6.5
   */
  public final List<Throwable> getExceptions() {
    if (this.exceptions == null) {
      this.exceptions = new ArrayList<Throwable>();
    }
    return this.exceptions;
  }

  /**
   * Adds the list of exceptions provided
   * 
   * @since GemFire 6.5
   */
  public final void addExceptions(Collection<? extends Throwable> ex) {
    getExceptions().addAll(ex);
  }
}
