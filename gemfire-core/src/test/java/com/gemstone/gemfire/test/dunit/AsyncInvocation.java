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
package com.gemstone.gemfire.test.dunit;

import java.util.concurrent.TimeoutException;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.SystemFailure;

// @todo davidw Add the ability to get a return value back from the
// async method call.  (Use a static ThreadLocal field that is
// accessible from the Runnable used in VM#invoke)
/**
 * <P>An <code>AsyncInvocation</code> represents the invocation of a
 * remote invocation that executes asynchronously from its caller.  An
 * instanceof <code>AsyncInvocation</code> provides information about
 * the invocation such as any exception that it may have thrown.</P>
 *
 * <P>Because it is a <code>Thread</code>, an
 * <code>AsyncInvocation</code> can be used as follows:</P>
 *
 * <PRE>
 *   AsyncInvocation ai1 = vm.invokeAsync(Test.class, "method1");
 *   AsyncInvocation ai2 = vm.invokeAsync(Test.class, "method2");
 *
 *   ai1.join();
 *   ai2.join();
 *
 *   assertTrue("Exception occurred while invoking " + ai1,
 *              !ai1.exceptionOccurred());
 *   if (ai2.exceptionOccurred()) {
 *     throw ai2.getException();
 *   }
 * </PRE>
 *
 * @see VM#invokeAsync(Class, String)
 */
public class AsyncInvocation<T> extends Thread {
  
  private static final ThreadLocal returnValue = new ThreadLocal();

  /** The singleton the thread group */
  private static final ThreadGroup GROUP = new AsyncInvocationGroup();

  ///////////////////// Instance Fields  /////////////////////

  /** An exception thrown while this async invocation ran */
  protected volatile Throwable exception;

  /** The object (or class) that is the receiver of this asyn method
   * invocation */
  private Object receiver;

  /** The name of the method being invoked */
  private String methodName;
  
  /** The returned object if any */
  public volatile T returnedObj = null;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>AsyncInvocation</code>
   *
   * @param receiver
   *        The object or {@link Class} on which the remote method was
   *        invoked
   * @param methodName
   *        The name of the method being invoked
   * @param work
   *        The actual invocation of the method
   */
  public AsyncInvocation(Object receiver, String methodName, Runnable work) {
    super(GROUP, work, getName(receiver, methodName));
    this.receiver = receiver;
    this.methodName = methodName;
    this.exception = null;
  }

  //////////////////////  Static Methods  /////////////////////

  /**
   * Returns the name of a <code>AsyncInvocation</code> based on its
   * receiver and method name.
   */
  private static String getName(Object receiver, String methodName) {
    StringBuffer sb = new StringBuffer(methodName);
    sb.append(" invoked on ");
    if (receiver instanceof Class) {
      sb.append("class ");
      sb.append(((Class) receiver).getName());

    } else {
      sb.append("an instance of ");
      sb.append(receiver.getClass().getName());
    }

    return sb.toString();
  }

  /////////////////////  Instance Methods  ////////////////////

  /**
   * Returns the receiver of this async method invocation
   */
  public Object getReceiver() {
    return this.receiver;
  }

  /**
   * Returns the name of the method being invoked remotely
   */
  public String getMethodName() {
    return this.methodName;
  }

  /**
   * Returns whether or not an exception occurred during this async
   * method invocation.
   */
  public boolean exceptionOccurred() {
    if (this.isAlive()) {
      throw new InternalGemFireError("Exception status not available while thread is alive.");
    }
    return this.exception != null;
  }

  /**
   * Returns the exception that was thrown during this async method
   * invocation.
   */
  public Throwable getException() {
    if (this.isAlive()) {
      throw new InternalGemFireError("Exception status not available while thread is alive.");
    }
    if (this.exception instanceof RMIException) {
      return ((RMIException) this.exception).getCause();

    } else {
      return this.exception;
    }
  }

  //////////////////////  Inner Classes  //////////////////////

  /**
   * A <code>ThreadGroup</code> that notices when an exception occurrs
   * during an <code>AsyncInvocation</code>.
   */
  private static class AsyncInvocationGroup extends ThreadGroup {
    AsyncInvocationGroup() {
      super("Async Invocations");
    }

    public void uncaughtException(Thread t, Throwable e) {
      if (e instanceof VirtualMachineError) {
        SystemFailure.setFailure((VirtualMachineError)e); // don't throw
      }
      if (t instanceof AsyncInvocation) {
        ((AsyncInvocation) t).exception = e;
      }
    }
  }
  
  public T getResult() throws Throwable {
    join();
    if(this.exceptionOccurred()) {
      throw new Exception("An exception occured during async invocation", this.exception);
    }
    return this.returnedObj;
  }
  
  public T getResult(long waitTime) throws Throwable {
    join(waitTime);
    if(this.isAlive()) {
      throw new TimeoutException();
    }
    if(this.exceptionOccurred()) {
      throw new Exception("An exception occured during async invocation", this.exception);
    }
    return this.returnedObj;
  }

  public T getReturnValue() {
    if (this.isAlive()) {
      throw new InternalGemFireError("Return value not available while thread is alive.");
    }
    return this.returnedObj;
  }
  
  public void run()
  {
    super.run();
    this.returnedObj = (T) returnValue.get();
    returnValue.set(null);
  }

  static void setReturnValue(Object v) {
    returnValue.set(v);
  }
}
