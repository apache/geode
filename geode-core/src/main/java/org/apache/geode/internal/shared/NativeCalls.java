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

package org.apache.geode.internal.shared;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.Immutable;

/**
 * Encapsulates native C/C++ calls via JNA. To obtain an instance of implementation for a platform,
 * use {@link NativeCalls#getInstance()}.
 *
 * @since GemFire 8.0
 */
public abstract class NativeCalls {

  /**
   * Static instance of NativeCalls implementation. This can be one of JNA implementations in
   * <code>NativeCallsJNAImpl</code> or can fall back to a generic implementation in case JNA is not
   * available for the platform.
   *
   * Note: this variable is deliberately not final so that other clients can plug in their own
   * native implementations of NativeCalls.
   */
  @Immutable
  protected static final NativeCalls instance;

  static {
    NativeCalls inst;
    try {
      // try to load JNA implementation first
      // we do it via reflection since some clients
      // may not have it
      final Class<?> c = Class.forName("org.apache.geode.internal.shared.NativeCallsJNAImpl");
      inst = (NativeCalls) c.getMethod("getInstance").invoke(null);
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      inst = null;
    }
    if (inst == null) {
      // fall back to generic implementation in case of a problem
      inst = new NativeCallsGeneric();
    }
    instance = inst;
  }

  public NativeCalls() {}

  /**
   * Get an instance of implementation of {@link NativeCalls} for the current platform.
   */
  public static NativeCalls getInstance() {
    return instance;
  }

  /**
   * Get the value of given environment variable. This is different from
   * {@link System#getenv(String)} in that it returns the current value of the environment variable
   * in the process rather than from a static unmodifiable map created on the first call.
   *
   * @param name the name of the environment variable to be modified
   */
  public abstract String getEnvironment(String name);

  /**
   * Get the process ID of the current process.
   */
  public abstract int getProcessId();

  /**
   * Check whether a process with given ID is still running.
   *
   * @throws UnsupportedOperationException if no native API to determine the process status could be
   *         invoked
   */
  public abstract boolean isProcessActive(int processId) throws UnsupportedOperationException;

  /**
   * Kill the process with given process ID immediately (i.e. without giving it a chance to cleanup
   * properly).
   *
   * @param processId the PID of the process to be kill
   *
   * @throws UnsupportedOperationException if no native API to kill the process could be invoked
   */
  public abstract boolean killProcess(int processId) throws UnsupportedOperationException;

  /**
   * Perform the steps necessary to make the current JVM a proper UNIX daemon.
   *
   * @param callback register callback to be invoked on catching a SIGHUP signal; SIGHUP signal is
   *        ignored if the callback is null
   *
   * @throws UnsupportedOperationException if the native calls could not be completed for some
   *         reason or are not available
   * @throws IllegalStateException for a non-UNIX platform
   */
  public void daemonize(RehashServerOnSIGHUP callback)
      throws UnsupportedOperationException, IllegalStateException {
    throw new UnsupportedOperationException("daemonize() not available in base implementation");
  }

  public void preBlow(String path, long maxSize, boolean preAllocate) throws IOException {
    try (RandomAccessFile raf = new RandomAccessFile(path, "rw")) {
      raf.setLength(maxSize);
    }
  }

  /**
   * This will return whether the path passed in as arg is part of a local file system or a remote
   * file system. This method is mainly used by the DiskCapacityMonitor thread and we don't want to
   * monitor remote fs available space as due to network problems/firewall issues the call to
   * getUsableSpace can hang. See bug #49155. On platforms other than Linux this will return false
   * even if it on local file system for now.
   */
  public boolean isOnLocalFileSystem(final String path) {
    return false;
  }

  /**
   * Callback invoked when an OS-level SIGHUP signal is caught after handler has been installed by
   * {@link NativeCalls#daemonize}. This is provided to allow for re-reading configuration files or
   * any other appropriate actions on receiving HUP signal as is the convention in other servers.
   *
   * @since GemFire 8.0
   */
  public interface RehashServerOnSIGHUP {

    /**
     * Perform the actions required to "rehash" the server.
     */
    void rehash();
  }

  /**
   * A generic fallback implementation of {@link NativeCalls} when no JNA based implementation could
   * be initialized (e.g. if JNA itself does not provide an implementation for the platform, or JNA
   * is not found).
   *
   * @since GemFire 8.0
   */
  public static class NativeCallsGeneric extends NativeCalls {

    @Override
    public String getEnvironment(final String name) {
      return System.getenv(name);
    }

    @Override
    public int getProcessId() {
      final String name = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
      final int idx = name.indexOf('@');
      if (idx > 0) {
        try {
          return Integer.parseInt(name.substring(0, idx));
        } catch (NumberFormatException nfe) {
          // something changed in the RuntimeMXBean name
        }
      }
      return 0;
    }

    @Override
    public boolean isProcessActive(int processId) throws UnsupportedOperationException {
      throw new UnsupportedOperationException(
          "isProcessActive() not available in generic implementation");
    }

    @Override
    public boolean killProcess(int processId) throws UnsupportedOperationException {
      throw new UnsupportedOperationException(
          "killProcess() not available in generic implementation");
    }

  }
}
