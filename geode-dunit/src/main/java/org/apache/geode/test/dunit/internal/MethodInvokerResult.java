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
package org.apache.geode.test.dunit.internal;

import static java.lang.System.lineSeparator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;

/**
 * The result of a {@link MethodInvoker} method invocation.
 */
public class MethodInvokerResult implements Serializable {

  /**
   * A "result" object that indicates that an exception occurred while invoking the method
   */
  public static final Serializable EXCEPTION_OCCURRED = new Serializable() {

    @Override
    public boolean equals(Object obj) {
      // Allows instances to be compared across VMs
      return obj != null && getClass().equals(obj.getClass());
    }

    @Override
    public int hashCode() {
      return getClass().hashCode();
    }

    @Override
    public String toString() {
      return "EXCEPTION_OCCURRED";
    }
  };

  /** The result of execution (may be an exception or error type) */
  private final Object result;

  /** The exception that resulted from invoking the method */
  private final Throwable exception;

  /** Stack trace information (if applicable) */
  private String stackTrace;

  MethodInvokerResult(Object result) {
    this.result = result;
    exception = null;
  }

  /**
   * This constructor is invoked when invoking a method resulted in an exception being thrown. The
   * "result" is set to {@link #EXCEPTION_OCCURRED}. If the exception could not be serialized,
   * {@link #getException()} will return IOException with the exception stack as the message.
   */
  MethodInvokerResult(Throwable throwable) {
    result = EXCEPTION_OCCURRED;
    exception = checkSerializable(throwable);
    stackTrace = toStackTrace(throwable);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getResult());
    sb.append(lineSeparator());
    if (getStackTrace() != null) {
      sb.append(getStackTrace());
    }
    return sb.toString();
  }

  /**
   * Returns the result of the method call. If an exception was thrown during the method call,
   * {@link #EXCEPTION_OCCURRED} is returned.
   *
   * @see #exceptionOccurred()
   */
  public Object getResult() {
    return result;
  }

  /**
   * Returns the stack trace of the exception that was thrown while invoking a method. If no
   * exception was thrown, <code>null</code> is returned.
   */
  public String getStackTrace() {
    return stackTrace;
  }

  /**
   * Returns the exception that was thrown while invoking a method. If no exception was thrown,
   * <code>null</code> is returned.
   */
  public Throwable getException() {
    return exception;
  }

  /**
   * Returns whether or not an exception occurred while invoking the method
   */
  public boolean exceptionOccurred() {
    return EXCEPTION_OCCURRED.equals(result);
  }

  private static String toStackTrace(Throwable throwable) {
    StringWriter sw = new StringWriter();
    throwable.printStackTrace(new PrintWriter(sw, true));
    return sw.toString();
  }

  private static Throwable checkSerializable(Throwable throwable) {
    try (ObjectOutputStream oos = new ObjectOutputStream(new ByteArrayOutputStream())) {
      oos.writeObject(throwable);
      return throwable;

    } catch (IOException e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw, true));
      return new IOException(sw.toString());
    }
  }
}
