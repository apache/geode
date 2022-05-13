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

import static org.apache.geode.test.dunit.internal.AsyncThreadId.nextId;
import static org.apache.geode.test.dunit.internal.DUnitLauncher.NUM_VMS;
import static org.apache.geode.test.version.VersionManager.isCurrentVersion;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.process.ProcessUtils;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.internal.ChildVMLauncher;
import org.apache.geode.test.dunit.internal.IdentifiableCallable;
import org.apache.geode.test.dunit.internal.IdentifiableRunnable;
import org.apache.geode.test.dunit.internal.MethodInvokerResult;
import org.apache.geode.test.dunit.internal.ProcessHolder;
import org.apache.geode.test.dunit.internal.RemoteDUnitVMIF;
import org.apache.geode.test.dunit.internal.StandAloneDUnitEnv;
import org.apache.geode.test.dunit.internal.VMEventNotifier;
import org.apache.geode.test.version.VersionManager;
import org.apache.geode.test.version.VmConfiguration;

/**
 * This class represents a Java Virtual Machine that runs in a DistributedTest.
 */
@SuppressWarnings("serial,unused")
public class VM implements Serializable {
  private static final Logger logger = LogService.getLogger();

  public static final int CONTROLLER_VM = -1;

  public static final int DEFAULT_VM_COUNT = NUM_VMS;

  private static final Object[] EMPTY = new Object[0];

  /** The host on which this VM runs */
  private final Host host;

  /** The sequential id of this VM */
  private final int id;

  /** The configuration (Java version + Geode version) used in this VM */
  private VmConfiguration configuration;

  /** The hydra client for this VM */
  private RemoteDUnitVMIF client;

  /** The state of this VM */
  private volatile boolean available;

  private transient volatile ProcessHolder processHolder;

  private final transient ChildVMLauncher childVMLauncher;

  /**
   * Returns the {@code VM} identity. For {@link StandAloneDUnitEnv} the number returned is a
   * zero-based sequence representing the order in with the DUnit {@code VM}s were launched.
   *
   * @return the id of the VM
   */
  public static int getVMId() {
    return DUnitEnv.get().getId();
  }

  /**
   * @return the id of the VM
   * @deprecated Please use {@link #getVMId()} instead.
   */
  @Deprecated
  public static int getCurrentVMNum() {
    return DUnitEnv.get().getId();
  }

  /*
   * Returns true if executed from the main JUnit VM.
   */
  public static boolean isControllerVM() {
    return getCurrentVMNum() == CONTROLLER_VM;
  }

  /*
   * Returns true if executed from a DUnit VM. Returns false if executed from the main JUnit VM.
   */
  public static boolean isVM() {
    return getCurrentVMNum() != CONTROLLER_VM;
  }

  /**
   * Returns a VM that runs in this DistributedTest.
   *
   * @param whichVM A zero-based identifier of the VM
   * @return a VM that runs in this DistributedTest.
   */
  public static VM getVM(int whichVM) {
    return Host.getHost(0).getVM(whichVM);
  }

  /**
   * Returns a VM running the specified Geode version in this DistributedTest.
   *
   * @param geodeVersion String specifying the Geode version
   * @param whichVM A zero-based identifier of the VM
   * @return a VM running the specified Geode version in this DistributedTest
   */
  public static VM getVM(String geodeVersion, int whichVM) {
    return getVM(VmConfiguration.forGeodeVersion(geodeVersion), whichVM);
  }

  public static VM getVM(VmConfiguration configuration, int whichVM) {
    return Host.getHost(0).getVM(configuration, whichVM);
  }

  /*
   * Returns a collection of all DistributedTest VMs.
   */
  public static List<VM> getAllVMs() {
    return Host.getHost(0).getAllVMs();
  }

  /*
   * Returns the number of VMs that run in this DistributedTest.
   */
  public static int getVMCount() {
    return Host.getHost(0).getVMCount();
  }

  /*
   * Returns the DistributedTest Locator VM.
   */
  public static VM getLocator() {
    return Host.getLocator();
  }

  /*
   * Returns the DistributedTest Locator VM.
   */
  public static VM getController() {
    return getVM(CONTROLLER_VM);
  }

  /*
   * Returns the machine name hosting this DistributedTest.
   */
  public static String getHostName() {
    return Host.getHost(0).getHostName();
  }

  /*
   * Returns the name of a VM for use in the RMI naming service or working directory on disk
   */
  public static String getVMName(final String version, final int pid) {
    if (pid == -2) {
      return "locator";
    }
    if (pid < 0 || isCurrentVersion(version)) {
      return "vm" + pid;
    } else {
      return "vm" + pid + "_v" + version;
    }
  }

  /*
   * Returns an array of all provided VMs.
   */
  public static VM[] toArray(VM... vms) {
    return vms;
  }

  /*
   * Returns an array of all provided VMs.
   */
  public static VM[] toArray(List<VM> vmList) {
    return vmList.toArray(new VM[0]);
  }

  /*
   * Returns an array of all provided VMs.
   */
  public static VM[] toArray(List<VM> vmList, VM... vms) {
    return ArrayUtils.addAll(vmList.toArray(new VM[0]), vms);
  }

  /*
   * Returns an array of all provided VMs.
   */
  public static VM[] toArray(VM[] vmArray, VM... vms) {
    return ArrayUtils.addAll(vmArray, vms);
  }

  /**
   * Registers a {@link VMEventListener}.
   *
   * @param listener the {@link VMEventListener} to register
   */
  public static void addVMEventListener(final VMEventListener listener) {
    getVMEventNotifier().addVMEventListener(listener);
  }

  /**
   * Deregisters a {@link VMEventListener}.
   *
   * @param listener the {@link VMEventListener} to deregister
   */
  public static void removeVMEventListener(final VMEventListener listener) {
    getVMEventNotifier().removeVMEventListener(listener);
  }

  public static String dumpThreads() {
    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    long[] allThreadIds = threadMXBean.getAllThreadIds();
    ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(allThreadIds, true, true);

    StringBuilder dumpWriter = new StringBuilder();
    Arrays.stream(threadInfos)
        .filter(Objects::nonNull)
        .forEach(dumpWriter::append);
    return dumpWriter.toString();
  }

  private static VMEventNotifier getVMEventNotifier() {
    return Host.getHost(0).getVMEventNotifier();
  }

  public VM(final Host host, VmConfiguration configuration, final int id,
      final RemoteDUnitVMIF client, final ProcessHolder processHolder,
      final ChildVMLauncher childVMLauncher) {
    this.host = host;
    this.id = id;
    this.configuration = configuration;
    this.client = client;
    this.processHolder = processHolder;
    this.childVMLauncher = childVMLauncher;
    available = true;
  }

  /**
   *
   * Returns the {@code Host} on which this {@code VM} is running.
   *
   * @return the {@code Host} on which this {@code VM} is running.
   */
  public Host getHost() {
    return host;
  }

  /**
   * @return the version of Geode used in this VM
   *
   * @see VersionManager#CURRENT_VERSION
   * @see Host#getVM(String, int)
   */
  public String getVersion() {
    return configuration.geodeVersion().toString();
  }

  /**
   * @return the VM configuration used in this VM
   */
  public VmConfiguration getConfiguration() {
    return configuration;
  }

  /**
   * Returns the VM id of this {@code VM}.
   *
   * @return the VM id of this {@code VM}.
   */
  public int getId() {
    return id;
  }

  /**
   * Returns the process id of this {@code VM}.
   *
   * @return the process id of this {@code VM}
   */
  public int getPid() {
    return invoke(() -> ProcessUtils.identifyPid());
  }

  /**
   * Invokes a static zero-arg method with an {@link Object} or {@code void} return type in this
   * {@code VM}. If the return type of the method is {@code void}, {@code null} is returned.
   *
   * @param <V> the type returned by the method
   * @param targetClass The class on which to invoke the method
   * @param methodName The name of the method to invoke
   * @return the object returned by the method
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
   * @param <V> the type returned by the method
   * @param targetClass The class on which to invoke the method
   * @param methodName The name of the method to invoke
   * @return an {@link AsyncInvocation}
   *
   * @deprecated Please use {@link #invokeAsync(SerializableCallableIF)} instead
   */
  @Deprecated
  public <V> AsyncInvocation<V> invokeAsync(final Class<?> targetClass, final String methodName) {
    return invokeAsync(targetClass, methodName, null);
  }

  /**
   * Invokes a static method with an {@link Object} or {@code void} return type in this VM. If the
   * return type of the method is {@code void}, {@code null} is returned.
   *
   * @param <V> the type returned by the method
   * @param targetClass The class on which to invoke the method
   * @param methodName The name of the method to invoke
   * @param args Arguments passed to the method call (must be {@link Serializable}).
   * @return the object returned by the method
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
   * @param <V> the type returned by the method
   * @param targetObject The object on which to invoke the method
   * @param methodName The name of the method to invoke
   * @param args Arguments passed to the method call (must be {@link Serializable}).
   * @return an {@link AsyncInvocation}
   *
   * @deprecated Please use {@link #invoke(SerializableCallableIF)} instead
   */
  @Deprecated
  public <V> AsyncInvocation<V> invokeAsync(final Object targetObject, final String methodName,
      final Object[] args) {
    return AsyncInvocation
        .<V>create(targetObject, methodName, () -> invoke(targetObject, methodName, args), this)
        .start();
  }

  /**
   * Asynchronously invokes an instance method with an {@link Object} or {@code void} return type in
   * this {@code VM}. If the return type of the method is {@code void}, {@code null} is returned.
   *
   * @param <V> the type returned by the method
   * @param targetClass The class on which to invoke the method
   * @param methodName The name of the method to invoke
   * @param args Arguments passed to the method call (must be {@link Serializable}).
   * @return an {@link AsyncInvocation}
   *
   * @deprecated Please use {@link #invoke(SerializableCallableIF)} instead
   */
  @Deprecated
  public <V> AsyncInvocation<V> invokeAsync(final Class<?> targetClass, final String methodName,
      final Object[] args) {
    return AsyncInvocation
        .<V>create(targetClass, methodName, () -> invoke(targetClass, methodName, args), this)
        .start();
  }

  /**
   * Invokes the {@code run} method of a {@link Runnable} in this VM. Recall that {@code run} takes
   * no arguments and has no return value.
   *
   * @param <V> the type returned by the AsyncInvocation
   * @param runnable The {@code Runnable} to be run
   * @return an {@link AsyncInvocation}
   *
   * @see SerializableRunnable
   */
  public <V> AsyncInvocation<V> invokeAsync(final SerializableRunnableIF runnable) {
    IdentifiableRunnable target = new IdentifiableRunnable(nextId(), runnable);
    return AsyncInvocation
        .<V>create(target, () -> invoke(target, target.getMethodName(), EMPTY), this).start();
  }

  /**
   * Invokes the {@code run} method of a {@link Runnable} in this VM. Recall that {@code run} takes
   * no arguments and has no return value. The {@code Runnable} is wrapped in a
   * {@link IdentifiableRunnable} having the given name so it shows up in DUnit logs.
   *
   * @param <V> the type returned by the AsyncInvocation
   * @param runnable The {@code Runnable} to be run
   * @param name The name of the {@code Runnable}, which will be logged in DUnit output
   * @return an {@link AsyncInvocation}
   *
   * @see SerializableRunnable
   */
  public <V> AsyncInvocation<V> invokeAsync(final String name,
      final SerializableRunnableIF runnable) {
    IdentifiableRunnable target = new IdentifiableRunnable(nextId(), name, runnable);
    return AsyncInvocation
        .<V>create(target, () -> invoke(target, target.getMethodName(), EMPTY), this).start();
  }

  /**
   * Invokes the {@code call} method of a {@link Callable} in this {@code VM}.
   *
   * @param <V> the type returned by the callable
   * @param callable The {@code Callable} to be run
   * @param name The name of the {@code Callable}, which will be logged in dunit output
   * @return an {@link AsyncInvocation}
   *
   * @see SerializableCallable
   */
  public <V> AsyncInvocation<V> invokeAsync(final String name,
      final SerializableCallableIF<V> callable) {
    IdentifiableCallable<V> target = new IdentifiableCallable<>(nextId(), name, callable);
    return AsyncInvocation.create(target, () -> invoke(target, target.getMethodName(), EMPTY), this)
        .start();
  }

  /**
   * Invokes the {@code call} method of a {@link Callable} in this {@code VM}.
   *
   * @param <V> the type returned by the callable
   * @param callable The {@code Callable} to be run
   * @return an {@link AsyncInvocation}
   *
   * @see SerializableCallable
   */
  public <V> AsyncInvocation<V> invokeAsync(final SerializableCallableIF<V> callable) {
    IdentifiableCallable<V> target = new IdentifiableCallable<>(nextId(), callable);
    return AsyncInvocation.create(target, () -> invoke(target, target.getMethodName(), EMPTY), this)
        .start();
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
    checkAvailability(IdentifiableRunnable.class.getName(), "run");
    executeMethodOnObject(new IdentifiableRunnable(name, runnable), "run", new Object[0]);
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
   * @param <V> the type returned by the callable
   * @param callable The {@code Callable} to be run
   * @param name The name of the {@code Callable}, which will be logged in DUnit output
   * @return the object returned by the callable
   *
   * @see SerializableCallable
   */
  public <V> V invoke(final String name, final SerializableCallableIF<V> callable) {
    checkAvailability(IdentifiableCallable.class.getName(), "call");
    return executeMethodOnObject(new IdentifiableCallable<>(name, callable), "call", new Object[0]);
  }

  /**
   * Invokes the {@code call} method of a {@link Callable} in this {@code VM}.
   *
   * @param <V> the type returned by the callable
   * @param callable The {@code Callable} to be run
   * @return the object returned by the callable
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
   * @param <V> the type returned by the method
   * @param targetObject The receiver of the method invocation
   * @param methodName The name of the method to invoke
   * @return the object returned by the method
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
   * @param <V> the type returned by the method
   * @param targetObject The receiver of the method invocation
   * @param methodName The name of the method to invoke
   * @param args Arguments passed to the method call (must be {@link Serializable}).
   * @return the object returned by the method
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
   * @return this VM
   * @throws RMIException if an exception occurs while bouncing this {@code VM}
   */
  public VM bounce() {
    return bounce(configuration, false);
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
   * @return this VM
   * @throws RMIException if an exception occurs while bouncing this {@code VM}
   */
  public VM bounceForcibly() {
    return bounce(configuration, true);
  }

  public VM bounce(final String targetVersion) {
    return bounce(VmConfiguration.forGeodeVersion(targetVersion), false);
  }

  public synchronized VM bounce(final VmConfiguration configuration, boolean force) {
    checkAvailability(getClass().getName(), "bounceVM");

    logger.info("Bouncing {} old pid is {} and configuration is {}", id, getPid(), configuration);
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

      // We typically try and use ephemeral ports everywhere. However, sometimes an ephemeral port
      // created during a prior start needs to be used as a fixed port on subsequent bounces. In
      // this case, ephemeral ports that are allocated early may end up conflicting with these
      // fixed ports. So when we bounce a VM, use an RMI port outside the usual range of ephemeral
      // ports for MacOS (49152–65535) and Linux (32768-60999).
      int remoteStubPort = AvailablePortHelper.getRandomAvailableTCPPort();
      processHolder = childVMLauncher.launchVM(configuration, id, true, remoteStubPort);
      this.configuration = configuration;
      client = childVMLauncher.getStub(id);
      available = true;

      logger.info("Bounced {}.  New pid is {} and configuration is {}", id, getPid(),
          configuration);
      getVMEventNotifier().notifyAfterBounceVM(this);

    } catch (InterruptedException | IOException | NotBoundException e) {
      throw new Error("Unable to restart VM " + id, e);
    }
    return this;
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
    return String.format("VM %d, Host %s, Geode %s, Java %s",
        getId(), getHost(), configuration.geodeVersion(), configuration.javaVersion());
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
