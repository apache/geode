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

import java.io.File;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.rmi.RemoteException;

import com.gemstone.gemfire.test.dunit.standalone.BounceResult;
import com.gemstone.gemfire.test.dunit.standalone.RemoteDUnitVMIF;

import hydra.MethExecutorResult;

/**
 * This class represents a Java Virtual Machine that runs on a host.
 *
 * @author David Whitlock
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

  ////////////////////  Constructors  ////////////////////

  /**
   * Creates a new <code>VM</code> that runs on a given host with a
   * given process id.
   */
  public VM(final Host host, final int pid, final RemoteDUnitVMIF client) {
    this.host = host;
    this.pid = pid;
    this.client = client;
    this.available = true;
  }

  //////////////////////  Accessors  //////////////////////

  /**
   * Returns the host on which this <code>VM</code> runs
   */
  public Host getHost() {
    return this.host;
  }

  /**
   * Returns the process id of this <code>VM</code>
   */
  public int getPid() {
    return this.pid;
  }

  /////////////////  Remote Method Invocation  ///////////////

  /**
   * Invokes a static zero-arg method  with an {@link Object} or
   * <code>void</code> return type in this VM.  If the return type of
   * the method is <code>void</code>, <code>null</code> is returned.
   *
   * @param targetClass
   *        The class on which to invoke the method
   * @param methodName
   *        The name of the method to invoke
   *
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public Object invoke(final Class targetClass, final String methodName) {
    return invoke(targetClass, methodName, new Object[0]);
  }

  /**
   * Asynchronously invokes a static zero-arg method with an {@link
   * Object} or <code>void</code> return type in this VM.  If the
   * return type of the method is <code>void</code>, <code>null</code>
   * is returned.
   *
   * @param targetClass
   *        The class on which to invoke the method
   * @param methodName
   *        The name of the method to invoke
   */
  public AsyncInvocation invokeAsync(final Class targetClass, final String methodName) {
    return invokeAsync(targetClass, methodName, null);
  }

  /**
   * Invokes a static method with an {@link Object} or
   * <code>void</code> return type in this VM.  If the return type of
   * the method is <code>void</code>, <code>null</code> is returned.
   *
   * @param targetClass
   *        The class on which to invoke the method
   * @param methodName
   *        The name of the method to invoke
   * @param args
   *        Arguments passed to the method call (must be {@link
   *        java.io.Serializable}). 
   *
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public Object invoke(Class targetClass, String methodName, Object[] args) {
    if (!this.available) {
      String s = "VM not available: " + this;
      throw new RMIException(this, targetClass.getName(), methodName,
            new IllegalStateException(s));
    }
    MethExecutorResult result = null;
    int retryCount = 120;
    do {
    try {
      result = this.client.executeMethodOnClass(targetClass.getName(), methodName, args);
      break; // out of while loop
    } catch( RemoteException e ) {
      boolean isWindows = false;
      String os = System.getProperty("os.name");
      if (os != null) {
        if (os.indexOf("Windows") != -1) {
          isWindows = true;
        }
      }
      if (isWindows && retryCount-- > 0) {
        boolean interrupted = Thread.interrupted();
        try { Thread.sleep(1000); } catch (InterruptedException ignore) {interrupted = true;}
        finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      } else {
        throw new RMIException(this, targetClass.getName(), methodName, e );
      }
    }
    } while (true);

    if (!result.exceptionOccurred()) {
      return result.getResult();

    } else {
      Throwable thr = result.getException();
      throw new RMIException(this, targetClass.getName(), methodName, thr,
                             result.getStackTrace()); 
    }
  }

  /**
   * Asynchronously invokes a static method with an {@link Object} or
   * <code>void</code> return type in this VM.  If the return type of
   * the method is <code>void</code>, <code>null</code> is returned.
   *
   * @param targetClass
   *        The class on which to invoke the method
   * @param methodName
   *        The name of the method to invoke
   * @param args
   *        Arguments passed to the method call (must be {@link
   *        java.io.Serializable}). 
   */
  public AsyncInvocation invokeAsync(final Class targetClass, 
                                     final String methodName,
                                     final Object[] args) {
    AsyncInvocation ai =
      new AsyncInvocation(targetClass, methodName, new Runnable() {
        public void run() {
          final Object o = invoke(targetClass, methodName, args);
          AsyncInvocation.setReturnValue(o);
        }
      });
    ai.start();
    return ai;
  }

  /**
   * Asynchronously invokes an instance method with an {@link Object} or
   * <code>void</code> return type in this VM.  If the return type of
   * the method is <code>void</code>, <code>null</code> is returned.
   *
   * @param o
   *        The object on which to invoke the method
   * @param methodName
   *        The name of the method to invoke
   * @param args
   *        Arguments passed to the method call (must be {@link
   *        java.io.Serializable}). 
   */
  public AsyncInvocation invokeAsync(final Object o, 
                                     final String methodName,
                                     final Object[] args) {
    AsyncInvocation ai =
      new AsyncInvocation(o, methodName, new Runnable() {
        public void run() {
          final Object ret = invoke(o, methodName, args);
          AsyncInvocation.setReturnValue(ret);
        }
      });
    ai.start();
    return ai;
  }

  /**
   * Invokes the <code>run</code> method of a {@link Runnable} in this
   * VM.  Recall that <code>run</code> takes no arguments and has no
   * return value.
   *
   * @param r
   *        The <code>Runnable</code> to be run
   *
   * @see SerializableRunnable
   */
  public AsyncInvocation invokeAsync(SerializableRunnableIF r) {
    return invokeAsync(r, "run", new Object[0]);
  }
  
  /**
   * Invokes the <code>call</code> method of a {@link Runnable} in this
   * VM.  
   *
   * @param c
   *        The <code>Callable</code> to be run
   *
   * @see SerializableCallable
   */
  public <T> AsyncInvocation<T> invokeAsync(SerializableCallableIF<T> c) {
    return invokeAsync(c, "call", new Object[0]);
  }

  /**
   * Invokes the <code>run</code> method of a {@link Runnable} in this
   * VM.  Recall that <code>run</code> takes no arguments and has no
   * return value.
   *
   * @param r
   *        The <code>Runnable</code> to be run
   *
   * @see SerializableRunnable
   */
  public void invoke(SerializableRunnableIF r) {
    invoke(r, "run");
  }
  
  /**
   * Invokes the <code>run</code> method of a {@link Runnable} in this
   * VM.  Recall that <code>run</code> takes no arguments and has no
   * return value.
   *
   * @param c
   *        The <code>Callable</code> to be run
   *
   * @see SerializableCallable
   */
  public <T>  T invoke(SerializableCallableIF<T> c) {
    return (T) invoke(c, "call");
  }
  
  /**
   * Invokes the <code>run</code> method of a {@link Runnable} in this
   * VM.  If the invocation throws AssertionFailedError, and repeatTimeoutMs
   * is >0, the <code>run</code> method is invoked repeatedly until it
   * either succeeds, or repeatTimeoutMs has passed.  The AssertionFailedError
   * is thrown back to the sender of this method if <code>run</code> has not
   * completed successfully before repeatTimeoutMs has passed.
   * 
   * @deprecated Please use {@link com.jayway.awaitility.Awaitility} with {@link #invoke(SerializableCallableIF)} instead.
   */
  public void invokeRepeatingIfNecessary(RepeatableRunnable o, long repeatTimeoutMs) {
    invoke(o, "runRepeatingIfNecessary", new Object[] {new Long(repeatTimeoutMs)});
  }

  /**
   * Invokes an instance method with no arguments on an object that is
   * serialized into this VM.  The return type of the method can be
   * either {@link Object} or <code>void</code>.  If the return type
   * of the method is <code>void</code>, <code>null</code> is
   * returned.
   *
   * @param o
   *        The receiver of the method invocation
   * @param methodName
   *        The name of the method to invoke
   *
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public Object invoke(Object o, String methodName) {
    return invoke(o, methodName, new Object[0]);
  }

  /**
   * Invokes an instance method on an object that is serialized into
   * this VM.  The return type of the method can be either {@link
   * Object} or <code>void</code>.  If the return type of the method
   * is <code>void</code>, <code>null</code> is returned.
   *
   * @param o
   *        The receiver of the method invocation
   * @param methodName
   *        The name of the method to invoke
   * @param args
   *        Arguments passed to the method call (must be {@link
   *        java.io.Serializable}). 
   *
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public Object invoke(Object o, String methodName, Object[] args) {
    if (!this.available) {
      String s = "VM not available: " + this;
      throw new RMIException(this, o.getClass().getName(), methodName,
            new IllegalStateException(s));
    }
    MethExecutorResult result = null;
    int retryCount = 120;
    do {
    try {
      if ( args == null )
        result = this.client.executeMethodOnObject(o, methodName);
      else
        result = this.client.executeMethodOnObject(o, methodName, args);
      break; // out of while loop
    } catch( RemoteException e ) {
      if (retryCount-- > 0) {
        boolean interrupted = Thread.interrupted();
        try { Thread.sleep(1000); } catch (InterruptedException ignore) {interrupted = true;}
        finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      } else {
        throw new RMIException(this, o.getClass().getName(), methodName, e );
      }
    }
    } while (true);

    if (!result.exceptionOccurred()) {
      return result.getResult();

    } else {
      Throwable thr = result.getException();
      throw new RMIException(this, o.getClass().getName(), methodName, thr,
                             result.getStackTrace()); 
    }
  }

  /**
   * Invokes the <code>main</code> method of a given class
   *
   * @param targetClass
   *        The class on which to invoke the <code>main</code> method
   * @param args
   *        The "command line" arguments to pass to the
   *        <code>main</code> method
   */
  public void invokeMain(Class targetClass, String[] args) {
    Object[] stupid = new Object[] { args };
    invoke(targetClass, "main", stupid);
  }

  /**
   * Asynchronously invokes the <code>main</code> method of a given
   * class
   *
   * @param c
   *        The class on which to invoke the <code>main</code> method
   * @param args
   *        The "command line" arguments to pass to the
   *        <code>main</code> method
   */
  public AsyncInvocation invokeMainAsync(Class c, String[] args) {
    Object[] stupid = new Object[] { args };
    return invokeAsync(c, "main", stupid);
  }

  /**
   * Invokes a static method with a <code>boolean</code> return type
   * in this VM.
   *
   * @param c
   *        The class on which to invoke the method
   * @param methodName
   *        The name of the method to invoke
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>boolean</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public boolean invokeBoolean(Class c, String methodName) {
    return invokeBoolean(c, methodName, new Object[0]);
  }

  /**
   * Invokes a static method with a <code>boolean</code> return type
   * in this VM.
   *
   * @param c
   *        The class on which to invoke the method
   * @param methodName
   *        The name of the method to invoke
   * @param args
   *        Arguments passed to the method call (must be {@link
   *        java.io.Serializable}). 
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>boolean</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public boolean invokeBoolean(Class c, String methodName, Object[] args) {
    Object result = invoke(c, methodName, args);
    if (result == null) {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned null, expected a boolean";
      throw new IllegalArgumentException(s);

    } else if (result instanceof Boolean) {
      return ((Boolean) result).booleanValue();

    } else {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned a \"" + result.getClass().getName()
        + "\" expected a boolean";
      throw new IllegalArgumentException(s);
    }
  }

  /**
   * Invokes an instance method on an object that is serialized into
   * this VM.  The return type of the method is <code>boolean</code>.
   *
   * @param o
   *        The receiver of the method invocation
   * @param methodName
   *        The name of the method to invoke
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>boolean</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public boolean invokeBoolean(Object o, String methodName) {
    return invokeBoolean(o, methodName, new Object[0]);
  }

  /**
   * Invokes an instance method on an object that is serialized into
   * this VM.  The return type of the method is <code>boolean</code>.
   *
   * @param o
   *        The receiver of the method invocation
   * @param methodName
   *        The name of the method to invoke
   * @param args
   *        Arguments passed to the method call (must be {@link
   *        java.io.Serializable}). 
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>boolean</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public boolean invokeBoolean(Object o, String methodName, Object[] args) {
    Object result = invoke(o, methodName, args);
    Class c = o.getClass();
    if (result == null) {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned null, expected a boolean";
      throw new IllegalArgumentException(s);

    } else if (result instanceof Boolean) {
      return ((Boolean) result).booleanValue();

    } else {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned a \"" + result.getClass().getName()
        + "\" expected a boolean";
      throw new IllegalArgumentException(s);
    }
  }

  /**
   * Invokes a static method with a <code>char</code> return type
   * in this VM.
   *
   * @param c
   *        The class on which to invoke the method
   * @param methodName
   *        The name of the method to invoke
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>char</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public char invokeChar(Class c, String methodName) {
    return invokeChar(c, methodName, new Object[0]);
  }

  /**
   * Invokes a static method with a <code>char</code> return type
   * in this VM.
   *
   * @param c
   *        The class on which to invoke the method
   * @param methodName
   *        The name of the method to invoke
   * @param args
   *        Arguments passed to the method call (must be {@link
   *        java.io.Serializable}). 
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>char</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public char invokeChar(Class c, String methodName, Object[] args) {
    Object result = invoke(c, methodName, args);
    if (result == null) {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned null, expected a char";
      throw new IllegalArgumentException(s);

    } else if (result instanceof Character) {
      return ((Character) result).charValue();

    } else {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned a \"" + result.getClass().getName()
        + "\" expected a char";
      throw new IllegalArgumentException(s);
    }
  }

  /**
   * Invokes an instance method on an object that is serialized into
   * this VM.  The return type of the method is <code>char</code>.
   *
   * @param o
   *        The receiver of the method invocation
   * @param methodName
   *        The name of the method to invoke
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>char</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public char invokeChar(Object o, String methodName) {
    return invokeChar(o, methodName, new Object[0]);
  }

  /**
   * Invokes an instance method on an object that is serialized into
   * this VM.  The return type of the method is <code>char</code>.
   *
   * @param o
   *        The receiver of the method invocation
   * @param methodName
   *        The name of the method to invoke
   * @param args
   *        Arguments passed to the method call (must be {@link
   *        java.io.Serializable}). 
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>char</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public char invokeChar(Object o, String methodName, Object[] args) {
    Object result = invoke(o, methodName, args);
    Class c = o.getClass();
    if (result == null) {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned null, expected a char";
      throw new IllegalArgumentException(s);

    } else if (result instanceof Character) {
      return ((Character) result).charValue();

    } else {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned a \"" + result.getClass().getName()
        + "\" expected a char";
      throw new IllegalArgumentException(s);
    }
  }

  /**
   * Invokes a static method with a <code>byte</code> return type
   * in this VM.
   *
   * @param c
   *        The class on which to invoke the method
   * @param methodName
   *        The name of the method to invoke
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>byte</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public byte invokeByte(Class c, String methodName) {
    return invokeByte(c, methodName, new Object[0]);
  }

  /**
   * Invokes a static method with a <code>byte</code> return type
   * in this VM.
   *
   * @param c
   *        The class on which to invoke the method
   * @param methodName
   *        The name of the method to invoke
   * @param args
   *        Arguments passed to the method call (must be {@link
   *        java.io.Serializable}). 
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>byte</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public byte invokeByte(Class c, String methodName, Object[] args) {
    Object result = invoke(c, methodName, args);
    if (result == null) {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned null, expected a byte";
      throw new IllegalArgumentException(s);

    } else if (result instanceof Byte) {
      return ((Byte) result).byteValue();

    } else {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned a \"" + result.getClass().getName()
        + "\" expected a byte";
      throw new IllegalArgumentException(s);
    }
  }

  /**
   * Invokes an instance method on an object that is serialized into
   * this VM.  The return type of the method is <code>byte</code>.
   *
   * @param o
   *        The receiver of the method invocation
   * @param methodName
   *        The name of the method to invoke
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>byte</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public byte invokeByte(Object o, String methodName) {
    return invokeByte(o, methodName, new Object[0]);
  }

  /**
   * Invokes an instance method on an object that is serialized into
   * this VM.  The return type of the method is <code>byte</code>.
   *
   * @param o
   *        The receiver of the method invocation
   * @param methodName
   *        The name of the method to invoke
   * @param args
   *        Arguments passed to the method call (must be {@link
   *        java.io.Serializable}). 
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>byte</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public byte invokeByte(Object o, String methodName, Object[] args) {
    Object result = invoke(o, methodName, args);
    Class c = o.getClass();
    if (result == null) {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned null, expected a byte";
      throw new IllegalArgumentException(s);

    } else if (result instanceof Byte) {
      return ((Byte) result).byteValue();

    } else {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned a \"" + result.getClass().getName()
        + "\" expected a byte";
      throw new IllegalArgumentException(s);
    }
  }

  /**
   * Invokes a static method with a <code>short</code> return type
   * in this VM.
   *
   * @param c
   *        The class on which to invoke the method
   * @param methodName
   *        The name of the method to invoke
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>short</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public short invokeShort(Class c, String methodName) {
    return invokeShort(c, methodName, new Object[0]);
  }

  /**
   * Invokes a static method with a <code>short</code> return type
   * in this VM.
   *
   * @param c
   *        The class on which to invoke the method
   * @param methodName
   *        The name of the method to invoke
   * @param args
   *        Arguments passed to the method call (must be {@link
   *        java.io.Serializable}). 
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>short</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public short invokeShort(Class c, String methodName, Object[] args) {
    Object result = invoke(c, methodName, args);
    if (result == null) {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned null, expected a short";
      throw new IllegalArgumentException(s);

    } else if (result instanceof Short) {
      return ((Short) result).shortValue();

    } else {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned a \"" + result.getClass().getName()
        + "\" expected a short";
      throw new IllegalArgumentException(s);
    }
  }

  /**
   * Invokes an instance method on an object that is serialized into
   * this VM.  The return type of the method is <code>short</code>.
   *
   * @param o
   *        The receiver of the method invocation
   * @param methodName
   *        The name of the method to invoke
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>short</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public short invokeShort(Object o, String methodName) {
    return invokeShort(o, methodName, new Object[0]);
  }

  /**
   * Invokes an instance method on an object that is serialized into
   * this VM.  The return type of the method is <code>short</code>.
   *
   * @param o
   *        The receiver of the method invocation
   * @param methodName
   *        The name of the method to invoke
   * @param args
   *        Arguments passed to the method call (must be {@link
   *        java.io.Serializable}). 
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>short</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public short invokeShort(Object o, String methodName, Object[] args) {
    Object result = invoke(o, methodName, args);
    Class c = o.getClass();
    if (result == null) {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned null, expected a short";
      throw new IllegalArgumentException(s);

    } else if (result instanceof Short) {
      return ((Short) result).shortValue();

    } else {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned a \"" + result.getClass().getName()
        + "\" expected a short";
      throw new IllegalArgumentException(s);
    }
  }

  /**
   * Invokes a static method with a <code>int</code> return type
   * in this VM.
   *
   * @param c
   *        The class on which to invoke the method
   * @param methodName
   *        The name of the method to invoke
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>int</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public int invokeInt(Class c, String methodName) {
    return invokeInt(c, methodName, new Object[0]);
  }

  /**
   * Invokes a static method with a <code>int</code> return type
   * in this VM.
   *
   * @param c
   *        The class on which to invoke the method
   * @param methodName
   *        The name of the method to invoke
   * @param args
   *        Arguments passed to the method call (must be {@link
   *        java.io.Serializable}). 
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>int</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public int invokeInt(Class c, String methodName, Object[] args) {
    Object result = invoke(c, methodName, args);
    if (result == null) {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned null, expected a int";
      throw new IllegalArgumentException(s);

    } else if (result instanceof Integer) {
      return ((Integer) result).intValue();

    } else {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned a \"" + result.getClass().getName()
        + "\" expected a int";
      throw new IllegalArgumentException(s);
    }
  }

  /**
   * Invokes an instance method on an object that is serialized into
   * this VM.  The return type of the method is <code>int</code>.
   *
   * @param o
   *        The receiver of the method invocation
   * @param methodName
   *        The name of the method to invoke
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>int</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public int invokeInt(Object o, String methodName) {
    return invokeInt(o, methodName, new Object[0]);
  }

  /**
   * Invokes an instance method on an object that is serialized into
   * this VM.  The return type of the method is <code>int</code>.
   *
   * @param o
   *        The receiver of the method invocation
   * @param methodName
   *        The name of the method to invoke
   * @param args
   *        Arguments passed to the method call (must be {@link
   *        java.io.Serializable}). 
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>int</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public int invokeInt(Object o, String methodName, Object[] args) {
    Object result = invoke(o, methodName, args);
    Class c = o.getClass();
    if (result == null) {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned null, expected a int";
      throw new IllegalArgumentException(s);

    } else if (result instanceof Integer) {
      return ((Integer) result).intValue();

    } else {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned a \"" + result.getClass().getName()
        + "\" expected a int";
      throw new IllegalArgumentException(s);
    }
  }

  /**
   * Invokes a static method with a <code>long</code> return type
   * in this VM.
   *
   * @param c
   *        The class on which to invoke the method
   * @param methodName
   *        The name of the method to invoke
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>long</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public long invokeLong(Class c, String methodName) {
    return invokeLong(c, methodName, new Object[0]);
  }

  /**
   * Invokes a static method with a <code>long</code> return type
   * in this VM.
   *
   * @param c
   *        The class on which to invoke the method
   * @param methodName
   *        The name of the method to invoke
   * @param args
   *        Arguments passed to the method call (must be {@link
   *        java.io.Serializable}). 
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>long</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public long invokeLong(Class c, String methodName, Object[] args) {
    Object result = invoke(c, methodName, args);
    if (result == null) {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned null, expected a long";
      throw new IllegalArgumentException(s);

    } else if (result instanceof Long) {
      return ((Long) result).longValue();

    } else {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned a \"" + result.getClass().getName()
        + "\" expected a long";
      throw new IllegalArgumentException(s);
    }
  }

  /**
   * Invokes an instance method on an object that is serialized into
   * this VM.  The return type of the method is <code>long</code>.
   *
   * @param o
   *        The receiver of the method invocation
   * @param methodName
   *        The name of the method to invoke
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>long</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public long invokeLong(Object o, String methodName) {
    return invokeLong(o, methodName, new Object[0]);
  }

  /**
   * Invokes an instance method on an object that is serialized into
   * this VM.  The return type of the method is <code>long</code>.
   *
   * @param o
   *        The receiver of the method invocation
   * @param methodName
   *        The name of the method to invoke
   * @param args
   *        Arguments passed to the method call (must be {@link
   *        java.io.Serializable}). 
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>long</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public long invokeLong(Object o, String methodName, Object[] args) {
    Object result = invoke(o, methodName, args);
    Class c = o.getClass();
    if (result == null) {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned null, expected a long";
      throw new IllegalArgumentException(s);

    } else if (result instanceof Long) {
      return ((Long) result).longValue();

    } else {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned a \"" + result.getClass().getName()
        + "\" expected a long";
      throw new IllegalArgumentException(s);
    }
  }

  /**
   * Invokes a static method with a <code>float</code> return type
   * in this VM.
   *
   * @param c
   *        The class on which to invoke the method
   * @param methodName
   *        The name of the method to invoke
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>float</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public float invokeFloat(Class c, String methodName) {
    return invokeFloat(c, methodName, new Object[0]);
  }

  /**
   * Invokes a static method with a <code>float</code> return type
   * in this VM.
   *
   * @param c
   *        The class on which to invoke the method
   * @param methodName
   *        The name of the method to invoke
   * @param args
   *        Arguments passed to the method call (must be {@link
   *        java.io.Serializable}). 
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>float</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public float invokeFloat(Class c, String methodName, Object[] args) {
    Object result = invoke(c, methodName, args);
    if (result == null) {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned null, expected a float";
      throw new IllegalArgumentException(s);

    } else if (result instanceof Float) {
      return ((Float) result).floatValue();

    } else {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned a \"" + result.getClass().getName()
        + "\" expected a float";
      throw new IllegalArgumentException(s);
    }
  }

  /**
   * Invokes an instance method on an object that is serialized into
   * this VM.  The return type of the method is <code>float</code>.
   *
   * @param o
   *        The receiver of the method invocation
   * @param methodName
   *        The name of the method to invoke
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>float</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public float invokeFloat(Object o, String methodName) {
    return invokeFloat(o, methodName, new Object[0]);
  }

  /**
   * Invokes an instance method on an object that is serialized into
   * this VM.  The return type of the method is <code>float</code>.
   *
   * @param o
   *        The receiver of the method invocation
   * @param methodName
   *        The name of the method to invoke
   * @param args
   *        Arguments passed to the method call (must be {@link
   *        java.io.Serializable}). 
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>float</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public float invokeFloat(Object o, String methodName, Object[] args) {
    Object result = invoke(o, methodName, args);
    Class c = o.getClass();
    if (result == null) {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned null, expected a float";
      throw new IllegalArgumentException(s);

    } else if (result instanceof Float) {
      return ((Float) result).floatValue();

    } else {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned a \"" + result.getClass().getName()
        + "\" expected a float";
      throw new IllegalArgumentException(s);
    }
  }

  /**
   * Invokes a static method with a <code>double</code> return type
   * in this VM.
   *
   * @param c
   *        The class on which to invoke the method
   * @param methodName
   *        The name of the method to invoke
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>double</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public double invokeDouble(Class c, String methodName) {
    return invokeDouble(c, methodName, new Object[0]);
  }

  /**
   * Invokes a static method with a <code>double</code> return type
   * in this VM.
   *
   * @param c
   *        The class on which to invoke the method
   * @param methodName
   *        The name of the method to invoke
   * @param args
   *        Arguments passed to the method call (must be {@link
   *        java.io.Serializable}). 
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>double</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public double invokeDouble(Class c, String methodName, Object[] args) {
    Object result = invoke(c, methodName, args);
    if (result == null) {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned null, expected a double";
      throw new IllegalArgumentException(s);

    } else if (result instanceof Double) {
      return ((Double) result).doubleValue();

    } else {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned a \"" + result.getClass().getName()
        + "\" expected a double";
      throw new IllegalArgumentException(s);
    }
  }

  /**
   * Invokes an instance method on an object that is serialized into
   * this VM.  The return type of the method is <code>double</code>.
   *
   * @param o
   *        The receiver of the method invocation
   * @param methodName
   *        The name of the method to invoke
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>double</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public double invokeDouble(Object o, String methodName) {
    return invokeDouble(o, methodName, new Object[0]);
  }

  /**
   * Invokes an instance method on an object that is serialized into
   * this VM.  The return type of the method is <code>double</code>.
   *
   * @param o
   *        The receiver of the method invocation
   * @param methodName
   *        The name of the method to invoke
   * @param args
   *        Arguments passed to the method call (must be {@link
   *        java.io.Serializable}). 
   *
   * @throws IllegalArgumentException
   *         The method does not return a <code>double</code>
   * @throws RMIException
   *         An exception occurred on while invoking the method in
   *         this VM
   */
  public double invokeDouble(Object o, String methodName, Object[] args) {
    Object result = invoke(o, methodName, args);
    Class c = o.getClass();
    if (result == null) {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned null, expected a double";
      throw new IllegalArgumentException(s);

    } else if (result instanceof Double) {
      return ((Double) result).doubleValue();

    } else {
      String s = "Method \"" + methodName + "\" in class \"" +
        c.getName() + "\" returned a \"" + result.getClass().getName()
        + "\" expected a double";
      throw new IllegalArgumentException(s);
    }
  }

  /**
   * Synchronously bounces (mean kills and restarts) this <code>VM</code>.
   * Concurrent bounce attempts are synchronized but attempts to invoke
   * methods on a bouncing VM will cause test failure.  Tests using bounce
   * should be placed at the end of the dunit test suite, since an exception
   * here will cause all tests using the unsuccessfully bounced VM to fail.
   * 
   * This method is currently not supported by the standalone dunit
   * runner.
   *
   * @throws RMIException if an exception occurs while bouncing this VM, for
   *  example a HydraTimeoutException if the VM fails to stop within 
   *  hydra.Prms#maxClientShutdownWaitSec or restart within 
   *  hydra.Prms#maxClientStartupWaitSec.
   */
  public synchronized void bounce() {
    if (!this.available) {
      String s = "VM not available: " + this;
      throw new RMIException(this, this.getClass().getName(), "bounceVM",
            new IllegalStateException(s));
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
      RMIException rmie = new RMIException(this, this.getClass().getName(),
        "bounceVM", e, sw.toString());
      throw rmie;
    }
  }

  /////////////////////  Utility Methods  ////////////////////

  public String toString() {
    return "VM " + this.getPid() + " running on " + this.getHost();
  }

  public static int getCurrentVMNum() {
    return DUnitEnv.get().getVMID();
  }
  
  public File getWorkingDirectory() {
    return DUnitEnv.get().getWorkingDirectory(this.getPid());
  }

  /** Return the total number of VMs on all hosts */
  public static int getVMCount() {
    int count = 0;
    for (int h = 0; h < Host.getHostCount(); h++) {
      Host host = Host.getHost(h);
      count += host.getVMCount();
    }
    return count;
  }

}
