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

import static org.apache.geode.test.dunit.standalone.DUnitLauncher.NUM_VMS;

import java.io.File;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.rmi.RemoteException;
import java.util.List;
import java.util.concurrent.Callable;

import hydra.MethExecutorResult;

import org.apache.geode.internal.process.ProcessUtils;
import org.apache.geode.test.dunit.standalone.BounceResult;
import org.apache.geode.test.dunit.standalone.RemoteDUnitVMIF;
import org.apache.geode.test.dunit.standalone.StandAloneDUnitEnv;
import org.apache.geode.test.dunit.standalone.VersionManager;

/**
 * This class represents a Java Virtual Machine that runs in a DistributedTest.
 */
@SuppressWarnings("serial,unused")
public class VM implements Serializable {

  public static final int CONTROLLER_VM = -1;

  public static final int DEFAULT_VM_COUNT = NUM_VMS;

  /** The host on which this VM runs */
  private final Host host;

  /** The sequential id of this VM */
  private int id;

  /** The version of Geode used in this VM */
  private String version;

  /** The hydra client for this VM */
  private RemoteDUnitVMIF client;

  /** The state of this VM */
  private volatile boolean available;

  /**
   * Returns the {@code VM} identity. For {@link StandAloneDUnitEnv} the number returned is a
   * zero-based sequence representing the order in with the DUnit {@code VM}s were launched.
   */
  public static int getCurrentVMNum() {
    return DUnitEnv.get().getVMID();
  }

  /**
   * Returns true if executed from the main JUnit VM.
   */
  public static boolean isControllerVM() {
    return getCurrentVMNum() == CONTROLLER_VM;
  }

  /**
   * Returns true if executed from a DUnit VM. Returns false if executed from the main JUnit VM.
   */
  public static boolean isVM() {
    return getCurrentVMNum() != CONTROLLER_VM;
  }

  /**
   * Returns a VM that runs in this DistributedTest.
   *
   * @param whichVM A zero-based identifier of the VM
   */
  public static VM getVM(int whichVM) {
    return Host.getHost(0).getVM(whichVM);
  }

  /**
   * Returns a collection of all DistributedTest VMs.
   */
  public static List<VM> getAllVMs() {
    return Host.getHost(0).getAllVMs();
  }

  /**
   * Returns the number of VMs that run in this DistributedTest.
   */
  public static int getVMCount() {
    return Host.getHost(0).getVMCount();
  }

  /**
   * Returns the DistributedTest Locator VM.
   */
  public static VM getLocator() {
    return Host.getLocator();
  }

  /**
   * Returns the DistributedTest Locator VM.
   */
  public static VM getController() {
    return getVM(CONTROLLER_VM);
  }

  /**
   * Returns the machine name hosting this DistributedTest.
   */
  public static String getHostName() {
    return Host.getHost(0).getHostName();
  }

  /**
   * Returns the name of a VM for use in the RMI naming service or working directory on disk
   */
  public static String getVMName(final String version, final int pid) {
    if (pid == -2) {
      return "locator";
    }
    if (pid < 0 || VersionManager.isCurrentVersion(version)) {
      return "vm" + pid;
    } else {
      return "vm" + pid + "_v" + version;
    }
  }

  /**
   * Returns an array of all provided VMs.
   */
  public static VM[] toArray(VM... vms) {
    return vms;
  }

  /**
   * Creates a new {@code VM} that runs on a given host with a given process id.
   */
  public VM(final Host host, final int id, final RemoteDUnitVMIF client) {
    this(host, VersionManager.CURRENT_VERSION, id, client);
  }

  public VM(final Host host, final String version, final int id, final RemoteDUnitVMIF client) {
    this.host = host;
    this.id = id;
    this.version = version;
    this.client = client;
    available = true;
  }

  /**
   * Returns the {@code Host} on which this {@code VM} is running.
   */
  public Host getHost() {
    return host;
  }

  /**
   * Returns the version of Geode used in this VM.
   *
   * @see VersionManager#CURRENT_VERSION
   * @see Host#getVM(String, int)
   */
  public String getVersion() {
    return version;
  }

  /**
   * Returns the VM id of this {@code VM}.
   */
  public int getId() {
    return id;
  }

  /**
   * Returns the process id of this {@code VM}.
   */
  public int getPid() {
    return invoke(() -> ProcessUtils.identifyPid());
  }

  /**
   * Invokes a static zero-arg method with an {@link Object} or {@code void} return type in this
   * {@code VM}. If the return type of the method is {@code void}, {@code null} is returned.
   *
   * @param targetClass The class on which to invoke the method
   * @param methodName The name of the method to invoke
   *
   * @throws RMIException Wraps any underlying exception thrown while invoking the method in this VM
   *
   * @deprecated Please use {@link #invoke(SerializableCallableIF)} instead
   */
  @Deprecated
  public <V> V invoke(final Class<?> targetClass, final String methodName) {
    return invoke(targetClass, methodName, new Object[0]);
  }

  /**
   * Asynchronously invokes a static zero-arg method with an {@code Object} or {@code void} return
   * type in this VM. If the return type of the method is {@code void}, {@code null} is returned.
   *
   * @param targetClass The class on which to invoke the method
   * @param methodName The name of the method to invoke
   *
   * @deprecated Please use {@link #invoke(SerializableCallableIF)} instead
   */
  @Deprecated
  public <V> AsyncInvocation<V> invokeAsync(final Class<?> targetClass, final String methodName) {
    return invokeAsync(targetClass, methodName, null);
  }

  /**
   * Invokes a static method with an {@link Object} or {@code void} return type in this VM. If the
   * return type of the method is {@code void}, {@code null} is returned.
   *
   * @param targetClass The class on which to invoke the method
   * @param methodName The name of the method to invoke
   * @param args Arguments passed to the method call (must be {@link java.io.Serializable}).
   *
   * @throws RMIException Wraps any underlying exception thrown while invoking the method in this
   *         {@code VM}
   *
   * @deprecated Please use {@link #invoke(SerializableCallableIF)} instead
   */
  @Deprecated
  public <V> V invoke(final Class<?> targetClass, final String methodName, final Object[] args) {
    if (!available) {
      throw new RMIException(this, targetClass.getName(), methodName,
          new IllegalStateException("VM not available: " + this));
    }

    MethExecutorResult result = execute(targetClass, methodName, args);

    if (!result.exceptionOccurred()) {
      return (V) result.getResult();

    } else {
      throw new RMIException(this, targetClass.getName(), methodName, result.getException(),
          result.getStackTrace());
    }
  }

  /**
   * Asynchronously invokes an instance method with an {@link Object} or {@code void} return type in
   * this {@code VM}. If the return type of the method is {@code void}, {@code null} is returned.
   *
   * @param targetObject The object on which to invoke the method
   * @param methodName The name of the method to invoke
   * @param args Arguments passed to the method call (must be {@link java.io.Serializable}).
   *
   * @deprecated Please use {@link #invoke(SerializableCallableIF)} instead
   */
  @Deprecated
  public <V> AsyncInvocation<V> invokeAsync(final Object targetObject, final String methodName,
      final Object[] args) {
    return new AsyncInvocation<V>(targetObject, methodName,
        () -> invoke(targetObject, methodName, args)).start();
  }

  /**
   * Asynchronously invokes an instance method with an {@link Object} or {@code void} return type in
   * this {@code VM}. If the return type of the method is {@code void}, {@code null} is returned.
   *
   * @param targetClass The class on which to invoke the method
   * @param methodName The name of the method to invoke
   * @param args Arguments passed to the method call (must be {@link java.io.Serializable}).
   *
   * @deprecated Please use {@link #invoke(SerializableCallableIF)} instead
   */
  @Deprecated
  public <V> AsyncInvocation<V> invokeAsync(final Class<?> targetClass, final String methodName,
      final Object[] args) {
    return new AsyncInvocation<V>(targetClass, methodName,
        () -> invoke(targetClass, methodName, args)).start();
  }

  /**
   * Invokes the {@code run} method of a {@link Runnable} in this VM. Recall that {@code run} takes
   * no arguments and has no return value.
   *
   * @param runnable The {@code Runnable} to be run
   *
   * @see SerializableRunnable
   */
  public <V> AsyncInvocation<V> invokeAsync(final SerializableRunnableIF runnable) {
    return invokeAsync(runnable, "run", new Object[0]);
  }

  /**
   * Invokes the {@code run} method of a {@link Runnable} in this VM. Recall that {@code run} takes
   * no arguments and has no return value. The {@code Runnable} is wrapped in a
   * {@link NamedRunnable} having the given name so it shows up in DUnit logs.
   *
   * @param runnable The {@code Runnable} to be run
   * @param name The name of the {@code Runnable}, which will be logged in DUnit output
   *
   * @see SerializableRunnable
   */
  public <V> AsyncInvocation<V> invokeAsync(final String name,
      final SerializableRunnableIF runnable) {
    return invokeAsync(new NamedRunnable(name, runnable), "run", new Object[0]);
  }

  /**
   * Invokes the {@code call} method of a {@link Callable} in this {@code VM}.
   *
   * @param callable The {@code Callable} to be run
   * @param name The name of the {@code Callable}, which will be logged in dunit output
   *
   * @see SerializableCallable
   */
  public <V> AsyncInvocation<V> invokeAsync(final String name,
      final SerializableCallableIF<V> callable) {
    return invokeAsync(new NamedCallable<>(name, callable), "call", new Object[0]);
  }

  /**
   * Invokes the {@code call} method of a {@link Callable} in this {@code VM}.
   *
   * @param callable The {@code Callable} to be run
   *
   * @see SerializableCallable
   */
  public <V> AsyncInvocation<V> invokeAsync(final SerializableCallableIF<V> callable) {
    return invokeAsync(callable, "call", new Object[0]);
  }

  /**
   * Invokes the {@code run} method of a {@link Runnable} in this {@code VM}. Recall that
   * {@code run} takes no arguments and has no return value.
   *
   * @param runnable The {@code Runnable} to be run
   * @param name The name of the {@code Runnable}, which will be logged in DUnit output
   *
   * @see SerializableRunnable
   */
  public void invoke(final String name, final SerializableRunnableIF runnable) {
    invoke(new NamedRunnable(name, runnable), "run");
  }

  /**
   * Invokes the {@code run} method of a {@link Runnable} in this {@code VM}. Recall that
   * {@code run} takes no arguments and has no return value.
   *
   * @param runnable The {@code Runnable} to be run
   *
   * @see SerializableRunnable
   */
  public void invoke(final SerializableRunnableIF runnable) {
    invoke(runnable, "run");
  }

  /**
   * Invokes the {@code call} method of a {@link Callable} in this {@code VM}.
   *
   * @param callable The {@code Callable} to be run
   * @param name The name of the {@code Callable}, which will be logged in DUnit output
   *
   * @see SerializableCallable
   */
  public <V> V invoke(final String name, final SerializableCallableIF<V> callable) {
    return invoke(new NamedCallable<>(name, callable), "call");
  }

  /**
   * Invokes the {@code call} method of a {@link Callable} in this {@code VM}.
   *
   * @param callable The {@code Callable} to be run
   *
   * @see SerializableCallable
   */
  public <V> V invoke(final SerializableCallableIF<V> callable) {
    return invoke(callable, "call");
  }

  /**
   * Invokes an instance method with no arguments on an object that is serialized into this
   * {@code VM}. The return type of the method can be either {@link Object} or {@code void}. If the
   * return type of the method is {@code void}, {@code null} is returned.
   *
   * @param targetObject The receiver of the method invocation
   * @param methodName The name of the method to invoke
   *
   * @throws RMIException Wraps any underlying exception thrown while invoking the method in this
   *         {@code VM}
   *
   * @deprecated Please use {@link #invoke(SerializableCallableIF)} instead.
   */
  @Deprecated
  public <V> V invoke(final Object targetObject, final String methodName) {
    return invoke(targetObject, methodName, new Object[0]);
  }

  /**
   * Invokes an instance method on an object that is serialized into this {@code VM}. The return
   * type of the method can be either {@link Object} or {@code void}. If the return type of the
   * method is {@code void}, {@code null} is returned.
   *
   * @param targetObject The receiver of the method invocation
   * @param methodName The name of the method to invoke
   * @param args Arguments passed to the method call (must be {@link java.io.Serializable}).
   *
   * @throws RMIException Wraps any underlying exception thrown while invoking the method in this
   *         {@code VM}
   *
   * @deprecated Please use {@link #invoke(SerializableCallableIF)} instead.
   */
  @Deprecated
  public <V> V invoke(final Object targetObject, final String methodName, final Object[] args) {
    if (!available) {
      throw new RMIException(this, targetObject.getClass().getName(), methodName,
          new IllegalStateException("VM not available: " + this));
    }

    MethExecutorResult result = execute(targetObject, methodName, args);

    if (!result.exceptionOccurred()) {
      return (V) result.getResult();

    } else {
      throw new RMIException(this, targetObject.getClass().getName(), methodName,
          result.getException(), result.getStackTrace());
    }
  }

  /**
   * Restart an unavailable VM
   */
  public synchronized void makeAvailable() {
    if (!available) {
      available = true;
      bounce();
    }
  }

  /**
   * Synchronously bounces (nice kill and restarts) this {@code VM}. Concurrent bounce attempts are
   * synchronized but attempts to invoke methods on a bouncing {@code VM} will cause test failure.
   * Tests using bounce should be placed at the end of the DUnit test suite, since an exception here
   * will cause all tests using the unsuccessfully bounced {@code VM} to fail.
   *
   * This method is currently not supported by the standalone DUnit runner.
   *
   * Note: bounce invokes shutdown hooks.
   *
   * @throws RMIException if an exception occurs while bouncing this {@code VM}
   */
  public void bounce() {
    bounce(version, false);
  }

  /**
   * Synchronously bounces (forced kill and restarts) this {@code VM}. Concurrent bounce attempts
   * are
   * synchronized but attempts to invoke methods on a bouncing {@code VM} will cause test failure.
   * Tests using bounce should be placed at the end of the DUnit test suite, since an exception here
   * will cause all tests using the unsuccessfully bounced {@code VM} to fail.
   *
   * This method is currently not supported by the standalone DUnit runner.
   *
   * Note: Forced bounce does not invoke shutdown hooks.
   *
   * @throws RMIException if an exception occurs while bouncing this {@code VM}
   */
  public void bounceForcibly() {
    bounce(version, true);
  }

  public void bounce(final String targetVersion) {
    bounce(targetVersion, false);
  }

  private synchronized void bounce(final String targetVersion, boolean force) {
    if (!available) {
      throw new RMIException(this, getClass().getName(), "bounceVM",
          new IllegalStateException("VM not available: " + this));
    }

    available = false;

    try {
      BounceResult result = DUnitEnv.get().bounce(targetVersion, id, force);
      id = result.getNewId();
      client = result.getNewClient();
      version = targetVersion;
      available = true;

    } catch (UnsupportedOperationException e) {
      available = true;
      throw e;

    } catch (RemoteException e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw, true));
      throw new RMIException(this, getClass().getName(), "bounceVM", e, sw.toString());
    }
  }

  public File getWorkingDirectory() {
    return DUnitEnv.get().getWorkingDirectory(getVersion(), getId());
  }

  @Override
  public String toString() {
    return "VM " + getId() + " running on " + getHost()
        + (VersionManager.isCurrentVersion(version) ? "" : (" with version " + version));
  }

  private MethExecutorResult execute(final Class<?> targetClass, final String methodName,
      final Object[] args) {
    try {
      return client.executeMethodOnClass(targetClass.getName(), methodName, args);
    } catch (RemoteException exception) {
      throw new RMIException(this, targetClass.getName(), methodName, exception);
    }
  }

  private MethExecutorResult execute(final Object targetObject, final String methodName,
      final Object[] args) {
    try {
      if (args == null) {
        return client.executeMethodOnObject(targetObject, methodName);
      } else {
        return client.executeMethodOnObject(targetObject, methodName, args);
      }
    } catch (RemoteException exception) {
      throw new RMIException(this, targetObject.getClass().getName(), methodName, exception);
    }
  }

}
