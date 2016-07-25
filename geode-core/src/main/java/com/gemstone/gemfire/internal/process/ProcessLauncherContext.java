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

package com.gemstone.gemfire.internal.process;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.io.TeeOutputStream;
import com.gemstone.gemfire.internal.io.TeePrintStream;

import java.io.*;
import java.util.Properties;

/**
 * Thread based context for launching a process. GemFire internals can acquire
 * optional configuration details from a process launcher via this context.
 * 
 * @since GemFire 7.0
 */
public final class ProcessLauncherContext {

  public static final String OVERRIDDEN_DEFAULTS_PREFIX = DistributionConfig.GEMFIRE_PREFIX + "default.";

  /**
   * Default value for {@link #isRedirectingOutput()}
   */
  private static final boolean REDIRECT_OUTPUT_DEFAULT = false;

  /**
   * Default value for {@link #getOverriddenDefaults()}
   */
  private static final Properties OVERRIDDEN_DEFAULTS_DEFAULT = new Properties();

  private static final ThreadLocal<ProcessLauncherContext> DATA =
    new ThreadLocal<ProcessLauncherContext>();

  private static ProcessLauncherContext get() {
    return DATA.get();
  }

  /**
   * Returns true if this process should redirect output to the system log.
   * @return true if this process should redirect output to the system log
   */
  public static boolean isRedirectingOutput() {
    final ProcessLauncherContext context = get();
    if (context == null) {
      return REDIRECT_OUTPUT_DEFAULT;
    }
    return context.redirectOutput();
  }

  /**
   * Returns the gemfire properties to be used if none of the specified
   * properties are defined by any other mechanism. This will only override
   * default values. If a property is defined by System property, API, or
   * within gemfire.properties then the contingent value will be ignored.
   * @return the contingent gemfire properties values to be used as an
   *         alternative default value
   */
  public static Properties getOverriddenDefaults() {
    final ProcessLauncherContext context = get();
    if (context == null) {
      return OVERRIDDEN_DEFAULTS_DEFAULT;
    }
    return context.overriddenDefaults();
  }


  public static StartupStatusListener getStartupListener() {
    final ProcessLauncherContext context = get();
    if (context == null) {
      return null;
    }

    return context.startupListener();
  }

  /**
   * Sets the ProcessLauncherContext data for the calling thread.
   */
  public static void set(final boolean redirectOutput,
                         final Properties contingentProperties,
                         final StartupStatusListener startupListener) {
    DATA.set(new ProcessLauncherContext(redirectOutput, contingentProperties, startupListener));
    installLogListener(startupListener);
  }

  /**
   * Clears the current ProcessLauncherContext for the calling thread.
   */
  public static void remove() {
    //DATA.get().restoreErrorStream();
    DATA.remove();
    clearLogListener();
  }

  private static void installLogListener(StartupStatusListener startupListener) {
    if (startupListener != null) {
      StartupStatus.setListener(startupListener);
    }
  }

  private static void clearLogListener() {
    StartupStatus.clearListener();
  }

  private final boolean redirectOutput;
  private final Properties overriddenDefaults;
  private final StartupStatusListener startupListener;
  private PrintStream err;

  private ProcessLauncherContext(final boolean redirectOutput,
                                 final Properties overriddenDefaults,
                                 final StartupStatusListener startupListener) {
    this.redirectOutput = redirectOutput;
    this.overriddenDefaults = overriddenDefaults;
    this.startupListener = startupListener;
  }

  private boolean redirectOutput() {
    return this.redirectOutput;
  }

  private Properties overriddenDefaults() {
    return this.overriddenDefaults;
  }

  private StartupStatusListener startupListener() {
    return this.startupListener;
  }
  
  @SuppressWarnings("unused")
  private void teeErrorStream() {
    final FileOutputStream fdErr = new FileOutputStream(FileDescriptor.err);
    this.err = new PrintStream(new BufferedOutputStream(fdErr, 128), true);
    System.setErr(new TeePrintStream(new TeeOutputStream(new BufferedOutputStream(fdErr, 128))));
  }

  @SuppressWarnings("unused")
  private void restoreErrorStream() {
    if (System.err instanceof TeePrintStream) {
      final TeePrintStream tee = ((TeePrintStream) System.err);
      final OutputStream branch = tee.getTeeOutputStream().getBranchOutputStream();

      PrintStream newStdErr = null;
      if (branch == null) {
        newStdErr = this.err;
      }
      else if (branch instanceof PrintStream) {
        newStdErr = (PrintStream) branch;
      }
      else {
        newStdErr = new PrintStream(new BufferedOutputStream(branch, 128), true);
      }
      System.setErr(newStdErr);
    }
  }
}
