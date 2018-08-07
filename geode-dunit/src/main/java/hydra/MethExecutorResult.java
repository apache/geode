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

package hydra;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;

/**
 *
 * The result of a MethExecutor execute method.
 *
 */
public class MethExecutorResult implements Serializable {

  /**
   * A "result" object that indicates that an exception occurred while invoking the method
   */
  public static final Serializable EXCEPTION_OCCURRED = new Serializable() {
    public boolean equals(Object o) {
      // Allows instances to be compared across VMs
      return o != null && this.getClass().equals(o.getClass());
    }

    public String toString() {
      return "EXCEPTION_OCCURRED";
    }
  };

  /**
   * A "exception" object that indicates that an exception could not be serialized.
   */
  public static final Throwable NONSERIALIZABLE_EXCEPTION = new Throwable() {
    public boolean equals(Object o) {
      // Allows instances to be compared across VMs
      return o != null && this.getClass().equals(o.getClass());
    }

    public String toString() {
      return "NONSERIALIZABLE_EXCEPTION";
    }
  };


  //////////////////// Instance Methods ///////////////////////////

  /** The result of execution (may be an exception or error type) */
  private Object result;

  /** The exception that resulted from invoking the method */
  private Throwable exception;

  /** Type of the exception (if applicable) */
  private String exceptionClassName;

  /** Message of the exception (if applicable) */
  private String exceptionMessage;

  /** Stack trace information (if applicable) */
  private String stackTrace;

  public MethExecutorResult() {
    this.result = null;
  }

  public MethExecutorResult(Object result) {
    this.result = result;
  }

  /**
   * This constructor is invoked when invoking a method resulted in an exception being thrown. The
   * "result" is set to {@link #EXCEPTION_OCCURRED}. If the exception could not be serialized,
   * {@link #getException()} will return IOException with the exception stack as the message.
   */
  public MethExecutorResult(Throwable thr) {
    this.result = EXCEPTION_OCCURRED;
    this.exceptionClassName = thr.getClass().getName();
    this.exceptionMessage = thr.getMessage();

    StringWriter sw = new StringWriter();

    thr.printStackTrace(new PrintWriter(sw, true));
    this.stackTrace = sw.toString();

    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(thr);
      this.exception = thr;

    } catch (IOException ex) {
      sw = new StringWriter();
      ex.printStackTrace(new PrintWriter(sw, true));
      this.exception = new IOException(sw.toString());
    }
  }

  public String toString() {
    StringBuffer s = new StringBuffer();
    s.append(this.getResult());
    s.append("\n");
    if (this.getStackTrace() != null) {
      s.append(this.getStackTrace());
    }
    return s.toString();
  }

  /**
   * Returns the result of the method call. If an exception was thrown during the method call,
   * {@link #EXCEPTION_OCCURRED} is returned.
   *
   * @see #exceptionOccurred()
   */
  public Object getResult() {
    return this.result;
  }

  /**
   * Returns the name of the exception class of the exception that was thrown while invoking a
   * method. If no exception was thrown, <code>null</code> is returned.
   */
  public String getExceptionClassName() {
    return this.exceptionClassName;
  }

  /**
   * Returns the message of the exception that was thrown while invoking a method. If no exception
   * was thrown, <code>null</code> is returned.
   */
  public String getExceptionMessage() {
    return this.exceptionMessage;
  }

  /**
   * Returns the stack trace of the exception that was thrown while invoking a method. If no
   * exception was thrown, <code>null</code> is returned.
   */
  public String getStackTrace() {
    return this.stackTrace;
  }

  /**
   * Returns the exception that was thrown while invoking a method. If the exception could not be
   * serialized, then {@link #NONSERIALIZABLE_EXCEPTION} is returned. If no exception was thrown,
   * <code>null</code> is returned.
   */
  public Throwable getException() {
    return this.exception;
  }

  /**
   * Returns whether or not an exception occurred while invoking the method
   */
  public boolean exceptionOccurred() {
    return EXCEPTION_OCCURRED.equals(this.result);
  }

}
