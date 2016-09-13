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
package org.apache.geode.test.dunit;

import java.io.File;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.rmi.RemoteException;
import java.util.concurrent.Callable;

import com.jayway.awaitility.Awaitility;
import hydra.MethExecutorResult;

import org.apache.geode.internal.process.ProcessUtils;
import org.apache.geode.test.dunit.standalone.BounceResult;
import org.apache.geode.test.dunit.standalone.RemoteDUnitVMIF;
import org.apache.geode.test.dunit.standalone.StandAloneDUnitEnv;

/**
 * This class represents a Java Virtual Machine that runs on a host.
 */
@SuppressWarnings("serial")
public class VM implements Serializable {

  /** The host on which this VM runs */
  private Host host;

  /** The process id of this VM */
  private int pid;

  /** The hydra client for this VM */
  private RemoteDUnitVMIF client;

  /** The state of this VM */
  private volatile boolean available;

  /**
   * Returns the {@code VM} identity. For {@link StandAloneDUnitEnv} the number
   * returned is a zero-based sequence representing the order in with
   * the DUnit {@code VM}s were launched.
   */
  public static int getCurrentVMNum() {
    return DUnitEnv.get().getVMID();
  }

  /**
   * Returns the total number of {@code VM}s on all {@code Host}s (note that
   * DUnit currently only supports one {@code Host}).
   */
  public static int getVMCount() {
    int count = 0;
    for (int h = 0; h < Host.getHostCount(); h++) {
      Host host = Host.getHost(h);
      count += host.getVMCount();
    }
    return count;
  }

  /**
   * Creates a new {@code VM} that runs on a given host with a given process
   * id.
   *
   * TODO: change pid to reflect value from {@link ProcessUtils#identifyPid()}
   */
  public VM(final Host host, final int pid, final RemoteDUnitVMIF client) {
    this.host = host;
    this.pid = pid;
    this.client = client;
    this.available = true;
  }

  /**
   * Returns the {@code Host} on which this {@code VM} is running.
   */
  public Host getHost() {
    return this.host;
  }

  /**
   * Returns the process id of this {@code VM}.
   */
  public int getPid() {
    return this.pid;
  }

  /**
   * Invokes a static zero-arg method  with an {@link Object} or {@code void}
   * return type in this {@code VM}.  If the return type of the method is
   * {@code void}, {@code null} is returned.
   *
   * @param  targetClass
   *         The class on which to invoke the method
   * @param  methodName
   *         The name of the method to invoke
   *
   * @throws RMIException
   *         Wraps any underlying exception thrown while invoking the method in
   *         this VM
   *
   * @deprecated Please use {@link #invoke(SerializableCallableIF)} instead
   */
  public Object invoke(final Class targetClass, final String methodName) {
    return invoke(targetClass, methodName, new Object[0]);
  }

  /**
   * Asynchronously invokes a static zero-arg method with an {@code Object} or
   * {@code void} return type in this VM.  If the return type of the method is
   * {@code void}, {@code null} is returned.
   *
   * @param  targetClass
   *         The class on which to invoke the method
   * @param  methodName
   *         The name of the method to invoke
   *
   * @deprecated Please use {@link #invoke(SerializableCallableIF)} instead
   */
  public AsyncInvocation invokeAsync(final Class targetClass, final String methodName) {
    return invokeAsync(targetClass, methodName, null);
  }

  /**
   * Invokes a static method with an {@link Object} or {@code void} return type
   * in this VM.  If the return type of the method is {@code void},
   * {@code null} is returned.
   *
   * @param  targetClass
   *         The class on which to invoke the method
   * @param  methodName
   *         The name of the method to invoke
   * @param  args
   *         Arguments passed to the method call (must be
   *         {@link java.io.Serializable}).
   *
   * @throws RMIException
   *         Wraps any underlying exception thrown while invoking the method in
   *         this {@code VM}
   *
   * @deprecated Please use {@link #invoke(SerializableCallableIF)} instead
   */
  public Object invoke(final Class targetClass, final String methodName, final Object[] args) {
    if (!this.available) {
      throw new RMIException(this, targetClass.getName(), methodName, new IllegalStateException("VM not available: " + this));
    }

    MethExecutorResult result = execute(targetClass, methodName, args);

    if (!result.exceptionOccurred()) {
      return result.getResult();

    } else {
      throw new RMIException(this, targetClass.getName(), methodName, result.getException(), result.getStackTrace());
    }
  }

  /**
   * Asynchronously invokes an instance method with an {@link Object} or
   * {@code void} return type in this {@code VM}.  If the return type of the
   * method is {@code void}, {@code null} is returned.
   *
   * @param  targetObject
   *         The object on which to invoke the method
   * @param  methodName
   *         The name of the method to invoke
   * @param  args
   *         Arguments passed to the method call (must be {@link
   *         java.io.Serializable}).
   *
   * @deprecated Please use {@link #invoke(SerializableCallableIF)} instead
   */
  public AsyncInvocation invokeAsync(final Object targetObject, final String methodName, final Object[] args) {
    return new AsyncInvocation(targetObject, methodName, () -> invoke(targetObject, methodName, args)).start();
  }

  /**
   * Asynchronously invokes an instance method with an {@link Object} or
   * {@code void} return type in this {@code VM}.  If the return type of the
   * method is {@code void}, {@code null} is returned.
   *
   * @param  targetClass
   *         The class on which to invoke the method
   * @param  methodName
   *         The name of the method to invoke
   * @param  args
   *         Arguments passed to the method call (must be {@link
   *         java.io.Serializable}).
   *
   * @deprecated Please use {@link #invoke(SerializableCallableIF)} instead
   */
  public AsyncInvocation invokeAsync(final Class<?> targetClass, final String methodName, final Object[] args) {
    return new AsyncInvocation(targetClass, methodName, () -> invoke(targetClass, methodName, args)).start();
  }

  /**
   * Invokes the {@code run} method of a {@link Runnable} in this VM.  Recall
   * that {@code run} takes no arguments and has no return value.
   *
   * @param  runnable
   *         The {@code Runnable} to be run
   *
   * @see SerializableRunnable
   */
  public AsyncInvocation invokeAsync(final SerializableRunnableIF runnable) {
    return invokeAsync(runnable, "run", new Object[0]);
  }
  
  /**
   * Invokes the {@code run} method of a {@link Runnable} in this VM.  Recall
   * that {@code run} takes no arguments and has no return value.  The
   * {@code Runnable} is wrapped in a {@link NamedRunnable} having the given
   * name so it shows up in DUnit logs.
   *
   * @param  runnable
   *         The {@code Runnable} to be run
   * @param  name
   *         The name of the {@code Runnable}, which will be logged in DUnit
   *         output
   *
   * @see SerializableRunnable
   */
  public AsyncInvocation invokeAsync(final String name, final SerializableRunnableIF runnable) {
    return invokeAsync(new NamedRunnable(name, runnable), "run", new Object[0]);
  }
  
  /**
   * Invokes the {@code call} method of a {@link Callable} in this {@code VM}.
   *
   * @param  callable
   *         The {@code Callable} to be run
   * @param  name
   *         The name of the {@code Callable}, which will be logged in dunit
   *         output
   *
   * @see SerializableCallable
   */
  public <T> AsyncInvocation<T> invokeAsync(final String name, final SerializableCallableIF<T> callable) {
    return invokeAsync(new NamedCallable(name, callable), "call", new Object[0]);
  }

  /**
   * Invokes the {@code call} method of a {@link Callable} in this {@code VM}.
   *
   * @param  callable
   *         The {@code Callable} to be run
   *
   * @see SerializableCallable
   */
  public <T> AsyncInvocation<T> invokeAsync(final SerializableCallableIF<T> callable) {
    return invokeAsync(callable, "call", new Object[0]);
  }

  /**
   * Invokes the {@code run} method of a {@link Runnable} in this {@code VM}.
   * Recall that {@code run} takes no arguments and has no return value.
   *
   * @param  runnable
   *         The {@code Runnable} to be run
   * @param  name
   *         The name of the {@code Runnable}, which will be logged in DUnit
   *         output
   *
   * @see SerializableRunnable
   */
  public void invoke(final String name, final SerializableRunnableIF runnable) {
    invoke(new NamedRunnable(name, runnable), "run");
  }

  /**
   * Invokes the {@code run} method of a {@link Runnable} in this {@code VM}.
   * Recall that {@code run} takes no arguments and has no return value.
   *
   * @param  runnable
   *         The {@code Runnable} to be run
   *
   * @see SerializableRunnable
   */
  public void invoke(final SerializableRunnableIF runnable) {
    invoke(runnable, "run");
  }
  
  /**
   * Invokes the {@code call} method of a {@link Callable} in this {@code VM}.
   *
   * @param  callable
   *         The {@code Callable} to be run
   * @param  name
   *         The name of the {@code Callable}, which will be logged in DUnit
   *         output
   *
   * @see SerializableCallable
   */
  public <T>  T invoke(final String name, final SerializableCallableIF<T> callable) {
    return (T) invoke(new NamedCallable(name, callable), "call");
  }
  
  /**
   * Invokes the {@code call} method of a {@link Callable} in this {@code VM}.
   *
   * @param  callable
   *         The {@code Callable} to be run
   *
   * @see SerializableCallable
   */
  public <T>  T invoke(final SerializableCallableIF<T> callable) {
    return (T) invoke(callable, "call");
  }
  
  /**
   * Invokes the {@code run} method of a {@link Runnable} in this {@code VM}.
   * If the invocation throws AssertionError, and repeatTimeoutMs
   * is >0, the {@code run} method is invoked repeatedly until it
   * either succeeds, or repeatTimeoutMs has passed.  The AssertionError
   * is thrown back to the sender of this method if {@code run} has not
   * completed successfully before repeatTimeoutMs has passed.
   * 
   * @deprecated Please use {@link Awaitility} to await condition and then {@link #invoke(SerializableCallableIF)} instead.
   */
  public void invokeRepeatingIfNecessary(final RepeatableRunnable runnable, final long repeatTimeoutMs) {
    invoke(runnable, "runRepeatingIfNecessary", new Object[] { repeatTimeoutMs });
  }

  /**
   * Invokes an instance method with no arguments on an object that is
   * serialized into this {@code VM}.  The return type of the method can be
   * either {@link Object} or {@code void}.  If the return type of the method
   * is {@code void}, {@code null} is returned.
   *
   * @param  targetObject
   *         The receiver of the method invocation
   * @param  methodName
   *         The name of the method to invoke
   *
   * @throws RMIException
   *         Wraps any underlying exception thrown while invoking the method in
   *         this {@code VM}
   *
   * @deprecated Please use {@link #invoke(SerializableCallableIF)} instead.
   */
  public Object invoke(final Object targetObject, final String methodName) {
    return invoke(targetObject, methodName, new Object[0]);
  }
  
  /**
   * Invokes an instance method on an object that is serialized into this
   * {@code VM}.  The return type of the method can be either {@link Object} or
   * {@code void}.  If the return type of the method is {@code void},
   * {@code null} is returned.
   *
   * @param  targetObject
   *         The receiver of the method invocation
   * @param  methodName
   *         The name of the method to invoke
   * @param  args
   *         Arguments passed to the method call (must be {@link
   *         java.io.Serializable}).
   *
   * @throws RMIException
   *         Wraps any underlying exception thrown while invoking the method in
   *         this {@code VM}
   *
   * @deprecated Please use {@link #invoke(SerializableCallableIF)} instead.
   */
  public Object invoke(final Object targetObject, final String methodName, final Object[] args) {
    if (!this.available) {
      throw new RMIException(this, targetObject.getClass().getName(), methodName, new IllegalStateException("VM not available: " + this));
    }

    MethExecutorResult result = execute(targetObject, methodName, args);

    if (!result.exceptionOccurred()) {
      return result.getResult();

    } else {
      throw new RMIException(this, targetObject.getClass().getName(), methodName, result.getException(), result.getStackTrace());
    }
  }

  /**
   * Synchronously bounces (mean kills and restarts) this {@code VM}.
   * Concurrent bounce attempts are synchronized but attempts to invoke methods
   * on a bouncing {@code VM} will cause test failure.  Tests using bounce
   * should be placed at the end of the DUnit test suite, since an exception
   * here will cause all tests using the unsuccessfully bounced {@code VM} to
   * fail.
   * 
   * This method is currently not supported by the standalone DUnit runner.
   *
   * @throws RMIException if an exception occurs while bouncing this
   *         {@code VM}, for example a {@code HydraTimeoutException} if the
   *         {@code VM} fails to stop within
   *         {@code hydra.Prms#maxClientShutdownWaitSec} or restart within
   *         {@code hydra.Prms#maxClientStartupWaitSec}.
   */
  public synchronized void bounce() {
    if (!this.available) {
      throw new RMIException(this, getClass().getName(), "bounceVM", new IllegalStateException("VM not available: " + this));
    }

    this.available = false;

    try {
      BounceResult result = DUnitEnv.get().bounce(this.pid);
      this.pid = result.getNewPid();
      this.client = result.getNewClient();
      this.available = true;

    } catch (UnsupportedOperationException e) {
      this.available = true;
      throw e;

    } catch (RemoteException e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw, true));
      RMIException rmie = new RMIException(this, getClass().getName(), "bounceVM", e, sw.toString());
      throw rmie;
    }
  }

  public String toString() {
    return "VM " + getPid() + " running on " + getHost();
  }

  public File getWorkingDirectory() {
    return DUnitEnv.get().getWorkingDirectory(getPid());
  }

  private MethExecutorResult execute(final Class targetClass, final String methodName, final Object[] args) {
    try {
      return this.client.executeMethodOnClass(targetClass.getName(), methodName, args);
    } catch (RemoteException exception) {
      throw new RMIException(this, targetClass.getName(), methodName, exception);
    }
  }

  private MethExecutorResult execute(final Object targetObject, final String methodName, final Object[] args) {
    try {
      if (args == null) {
        return this.client.executeMethodOnObject(targetObject, methodName);
      } else {
        return this.client.executeMethodOnObject(targetObject, methodName, args);
      }
    } catch (RemoteException exception) {
      throw new RMIException(this, targetObject.getClass().getName(), methodName, exception);
    }
  }
}
