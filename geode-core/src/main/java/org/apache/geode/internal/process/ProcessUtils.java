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
package org.apache.geode.internal.process;

import static org.apache.commons.lang3.Validate.isTrue;
import static org.apache.commons.lang3.Validate.notEmpty;
import static org.apache.commons.lang3.Validate.notNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.management.ManagementFactory;

import org.apache.geode.annotations.Immutable;

/**
 * Utility operations for processes such as identifying the process id (pid).
 *
 * @since GemFire 7.0
 */
public class ProcessUtils {

  @Immutable
  private static final InternalProcessUtils internal = initializeInternalProcessUtils();

  private ProcessUtils() {
    // nothing
  }

  /**
   * Returns the pid for this process.
   *
   * @throws PidUnavailableException if parsing the pid from the name of the RuntimeMXBean fails
   *
   * @see java.lang.management.RuntimeMXBean#getName()
   */
  public static int identifyPid() throws PidUnavailableException {
    return identifyPid(ManagementFactory.getRuntimeMXBean().getName());
  }

  /**
   * Returns the pid for this process without throwing a checked exception.
   *
   * @throws UncheckedPidUnavailableException if parsing the pid from the name of the RuntimeMXBean
   *         fails
   *
   * @see java.lang.management.RuntimeMXBean#getName()
   */
  public static int identifyPidAsUnchecked() throws UncheckedPidUnavailableException {
    try {
      return identifyPid();
    } catch (PidUnavailableException e) {
      throw new UncheckedPidUnavailableException(e);
    }
  }

  /**
   * Returns the pid for this process using the specified name from RuntimeMXBean.
   *
   * @throws PidUnavailableException if parsing the pid from the RuntimeMXBean name fails
   */
  public static int identifyPid(final String name) throws PidUnavailableException {
    notEmpty(name, "Invalid name '" + name + "' specified");

    try {
      final int index = name.indexOf('@');
      if (index < 0) {
        throw new PidUnavailableException("Unable to parse pid from " + name);
      }
      return Integer.parseInt(name.substring(0, index));
    } catch (NumberFormatException e) {
      throw new PidUnavailableException("Unable to parse pid from " + name, e);
    }
  }

  /**
   * Returns true if a process identified by the process id is currently running on this host
   * machine.
   *
   * @param pid process id to check for
   * @return true if the pid matches a currently running process
   */
  public static boolean isProcessAlive(final int pid) {
    isTrue(pid > 0, "Invalid pid '" + pid + "' specified");

    return internal.isProcessAlive(pid);
  }

  /**
   * Returns true if a process identified by the specified Process is currently running on this host
   * machine.
   *
   * @param process the Process to check
   * @return true if the Process is a currently running process
   */
  public static boolean isProcessAlive(final Process process) {
    notNull(process, "Invalid process '" + process + "' specified");

    return process.isAlive();
  }

  /**
   * Returns true if a process identified by the process id was running on this host machine and has
   * been terminated by this operation.
   *
   * @param pid process id
   * @return true if the process was terminated by this operation
   */
  public static boolean killProcess(final int pid) {
    isTrue(pid > 0, "Invalid pid '" + pid + "' specified");

    return internal.killProcess(pid);
  }

  public static int readPid(final File pidFile) throws IOException {
    notNull(pidFile, "Invalid pidFile '" + pidFile + "' specified");
    isTrue(pidFile.exists(), "Nonexistent pidFile '" + pidFile + "' specified");

    try (BufferedReader reader = new BufferedReader(new FileReader(pidFile))) {
      return Integer.parseInt(reader.readLine());
    }
  }

  /**
   * Returns true if Attach API or JNA NativeCalls is available for killing process or checking if
   * it is alive.
   */
  public static boolean isAvailable() {
    return internal.isAvailable();
  }

  /**
   * Returns true if Attach API is available for checking status.
   */
  public static boolean isAttachApiAvailable() {
    return internal.isAttachApiAvailable();
  }

  private static InternalProcessUtils initializeInternalProcessUtils() {
    // 1) prefer Attach because it filters out non-JVM processes
    try {
      Class.forName("com.sun.tools.attach.VirtualMachine");
      Class.forName("com.sun.tools.attach.VirtualMachineDescriptor");
      return new AttachProcessUtils();
    } catch (ClassNotFoundException | LinkageError ignored) {
      // fall through
    }

    // 2) try NativeCalls but make sure it doesn't throw UnsupportedOperationException
    try {
      // consider getting rid of Class.forName usage if NativeCalls always safely loads
      Class.forName("org.apache.geode.internal.shared.NativeCalls");
      NativeProcessUtils nativeProcessUtils = new NativeProcessUtils();
      boolean result = nativeProcessUtils.isProcessAlive(identifyPid());
      if (result) {
        return nativeProcessUtils;
      }
    } catch (ClassNotFoundException | LinkageError | PidUnavailableException
        | UnsupportedOperationException ignored) {
      // fall through
    }

    // 3) consider logging warning and then proceed with no-op
    return new InternalProcessUtils() {
      @Override
      public boolean isProcessAlive(final int pid) {
        return false;
      }

      @Override
      public boolean killProcess(final int pid) {
        return false;
      }

      @Override
      public boolean isAvailable() {
        return false;
      }

      @Override
      public boolean isAttachApiAvailable() {
        return false;
      }
    };
  }

  /**
   * Defines the SPI for ProcessUtils
   */
  interface InternalProcessUtils {

    boolean isProcessAlive(final int pid);

    boolean killProcess(final int pid);

    boolean isAvailable();

    boolean isAttachApiAvailable();
  }
}
