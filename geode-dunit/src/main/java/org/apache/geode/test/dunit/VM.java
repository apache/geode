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

import static org.apache.geode.test.dunit.internal.DUnitLauncher.NUM_VMS;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.process.ProcessUtils;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.internal.ChildVMLauncher;
import org.apache.geode.test.dunit.internal.MethodInvokerResult;
import org.apache.geode.test.dunit.internal.ProcessHolder;
import org.apache.geode.test.dunit.internal.RemoteDUnitVMIF;
import org.apache.geode.test.dunit.internal.StandAloneDUnitEnv;
import org.apache.geode.test.dunit.internal.VMEventNotifier;
import org.apache.geode.test.version.VersionManager;

/**
 * This class represents a Java Virtual Machine that runs in a DistributedTest.
 */
@SuppressWarnings("serial,unused")
public class VM implements Serializable {

  private static final Logger logger = LogService.getLogger();

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

  private transient volatile ProcessHolder processHolder;

  private final transient ChildVMLauncher childVMLauncher;

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
   * Returns an array of all provided VMs.
   */
  public static VM[] toArray(List<VM> vmList) {
    return vmList.toArray(new VM[0]);
  }

  /**
   * Returns an array of all provided VMs.
   */
  public static VM[] toArray(List<VM> vmList, VM... vms) {
    return ArrayUtils.addAll(vmList.toArray(new VM[0]), vms);
  }

  /**
   * Returns an array of all provided VMs.
   */
  public static VM[] toArray(VM[] vmArray, VM... vms) {
    return ArrayUtils.addAll(vmArray, vms);
  }

  /**
   * Registers a {@link VMEventListener}.
   */
  public static void addVMEventListener(final VMEventListener listener) {
    getVMEventNotifier().addVMEventListener(listener);
  }

  /**
   * Deregisters a {@link VMEventListener}.
   */
  public static void removeVMEventListener(final VMEventListener listener) {
    getVMEventNotifier().removeVMEventListener(listener);
  }

  private static VMEventNotifier getVMEventNotifier() {
    return Host.getHost(0).getVMEventNotifier();
  }

  /**
   * Creates a new {@code VM} that runs on a given host with a given process id.
   */
  public VM(final Host host, final int id, final RemoteDUnitVMIF client,
      final ProcessHolder processHolder, final ChildVMLauncher childVMLauncher) {
    this(host, VersionManager.CURRENT_VERSION, id, client, processHolder, childVMLauncher);
  }

  public VM(final Host host, final String version, final int id, final RemoteDUnitVMIF client,
      final ProcessHolder processHolder, final ChildVMLauncher childVMLauncher) {
    this.host = host;
    this.id = id;
    this.version = version;
    this.client = client;
    this.processHolder = processHolder;
    this.childVMLauncher = childVMLauncher;
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
    checkAvailability(targetClass.getName(), methodName);
    return executeMethodOnClass(targetClass, methodName, new Object[0]);
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
    checkAvailability(targetClass.getName(), methodName);
    return executeMethodOnClass(targetClass, methodName, args);
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
    checkAvailability(NamedRunnable.class.getName(), "run");
    executeMethodOnObject(new NamedRunnable(name, runnable), "run", new Object[0]);
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
    checkAvailability(runnable.getClass().getName(), "run");
    executeMethodOnObject(runnable, "run", new Object[0]);
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
    checkAvailability(NamedCallable.class.getName(), "call");
    return executeMethodOnObject(new NamedCallable<>(name, callable), "call", new Object[0]);
  }

  /**
   * Invokes the {@code call} method of a {@link Callable} in this {@code VM}.
   *
   * @param callable The {@code Callable} to be run
   *
   * @see SerializableCallable
   */
  public <V> V invoke(final SerializableCallableIF<V> callable) {
    checkAvailability(callable.getClass().getName(), "call");
    return executeMethodOnObject(callable, "call", new Object[0]);
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
    checkAvailability(targetObject.getClass().getName(), methodName);
    return executeMethodOnObject(targetObject, methodName, new Object[0]);
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
    checkAvailability(targetObject.getClass().getName(), methodName);
    return executeMethodOnObject(targetObject, methodName, args);
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
    checkAvailability(getClass().getName(), "bounceVM");

    logger.info("Bouncing {} old pid is {}", id, getPid());
    getVMEventNotifier().notifyBeforeBounceVM(this);

    available = false;
    try {
      if (force) {
        processHolder.killForcibly();
      } else {
        SerializableRunnableIF runnable = () -> new Thread(() -> {
          try {
            // sleep before exit so that the rmi call is returned
            Thread.sleep(100);
            System.exit(0);
          } catch (InterruptedException e) {
            logger.error("VM bounce thread interrupted before exiting.", e);
          }
        }).start();
        executeMethodOnObject(runnable, "run", new Object[0]);
      }
      processHolder.waitFor();
      processHolder = childVMLauncher.launchVM(targetVersion, id, true);
      version = targetVersion;
      client = childVMLauncher.getStub(id);
      available = true;

      logger.info("Bounced {} new pid is {}", id, getPid());
      getVMEventNotifier().notifyAfterBounceVM(this);

    } catch (InterruptedException | IOException | NotBoundException e) {
      throw new Error("Unable to restart VM " + id, e);
    }
  }

  private void checkAvailability(String className, String methodName) {
    if (!available) {
      throw new RMIException(this, className, methodName,
          new IllegalStateException("VM not available: " + this));
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

  private <V> V executeMethodOnObject(final Object targetObject, final String methodName,
      final Object[] args) {
    try {
      MethodInvokerResult result = client.executeMethodOnObject(targetObject, methodName, args);
      if (result.exceptionOccurred()) {
        throw new RMIException(this, targetObject.getClass().getName(), methodName,
            result.getException(), result.getStackTrace());
      }
      return (V) result.getResult();
    } catch (RemoteException exception) {
      throw new RMIException(this, targetObject.getClass().getName(), methodName, exception);
    }
  }

  private <V> V executeMethodOnClass(final Class<?> targetClass, final String methodName,
      final Object[] args) {
    try {
      MethodInvokerResult result =
          client.executeMethodOnClass(targetClass.getName(), methodName, args);
      if (result.exceptionOccurred()) {
        throw new RMIException(this, targetClass.getName(), methodName, result.getException(),
            result.getStackTrace());
      }
      return (V) result.getResult();
    } catch (RemoteException exception) {
      throw new RMIException(this, targetClass.getName(), methodName, exception);
    }
  }
}
