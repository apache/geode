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
package org.apache.geode.test.dunit;

import java.io.PrintWriter;

import org.apache.geode.GemFireException;

/**
 * This exception is thrown when an exception occurs during a remote method invocation. This
 * {@link RuntimeException} wraps the actual exception. It allows distributed unit tests to verify
 * that an exception was thrown in a different VM.
 *
 * <pre>
 *     VM vm0 = host0.getVM(0);
 *     try {
 *       vm.invoke(() -> this.getUnknownObject());
 *
 *     } catch (RMIException ex) {
 *       assertIndexDetailsEquals(ex.getCause() instanceof ObjectException);
 *     }
 * </pre>
 *
 * <p>
 * Note that special steps are taken so that the stack trace of the cause exception reflects the
 * call stack on the remote machine. The stack trace of the exception returned by
 * {@link #getCause()} may not be available.
 */
@SuppressWarnings("serial")
public class RMIException extends GemFireException {

  /**
   * SHADOWED FIELD that holds the cause exception (as opposed to the HokeyException
   */
  private final Throwable cause;

  /** The name of the method being invoked */
  private final String methodName;

  /**
   * The name of the class (or class of the object type) whose method was being invoked
   */
  private final String className;

  /** The VM in which the method was executing */
  private final VM vm;

  /**
   * Creates a new {@code RMIException} that was caused by a given {@code Throwable} while
   * invoking a given method.
   */
  public RMIException(VM vm, String className, String methodName, Throwable cause) {
    super("While invoking " + className + "." + methodName + " in " + vm, cause);
    this.cause = cause;
    this.className = className;
    this.methodName = methodName;
    this.vm = vm;
  }

  /**
   * Creates a new {@code RMIException} to indicate that an exception of a given type was
   * thrown while invoking a given method.
   *
   * @param vm The VM in which the method was executing
   * @param className The name of the class whose method was being invoked remotely
   * @param methodName The name of the method that was being invoked remotely
   * @param cause The type of exception that was thrown in the remote VM
   * @param stackTrace The stack trace of the exception from the remote VM
   */
  public RMIException(VM vm, String className, String methodName, Throwable cause,
      String stackTrace) {
    super("While invoking " + className + "." + methodName + " in " + vm,
        new HokeyException(cause, stackTrace));
    this.vm = vm;
    this.cause = cause;
    this.className = className;
    this.methodName = methodName;
  }

  /**
   * Returns the cause of this exception. Note that this is not necessarily the exception that gets
   * printed out with the stack trace.
   */
  @Override
  public Throwable getCause() {
    return cause;
  }

  /**
   * Returns the VM in which the remote method was invoked
   */
  public VM getVM() {
    return vm;
  }

  /**
   * A hokey exception class that makes it looks like we have a real cause exception.
   */
  private static class HokeyException extends Throwable {

    private final String stackTrace;
    private final String toString;

    HokeyException(Throwable cause, String stackTrace) {
      toString = cause.toString();
      this.stackTrace = stackTrace;
    }

    @Override
    public void printStackTrace(PrintWriter pw) {
      pw.print(stackTrace);
      pw.flush();
    }

    public String toString() {
      return toString;
    }
  }
}
