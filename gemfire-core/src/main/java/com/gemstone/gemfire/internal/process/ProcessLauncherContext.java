/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.process;

import java.io.BufferedOutputStream;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Properties;

import com.gemstone.gemfire.internal.io.TeeOutputStream;
import com.gemstone.gemfire.internal.io.TeePrintStream;

/**
 * Thread based context for launching a process. GemFire internals can acquire
 * optional configuration details from a process launcher via this context.
 * 
 * @author Kirk Lund
 * @since 7.0
 */
public final class ProcessLauncherContext {

  public static final String OVERRIDDEN_DEFAULTS_PREFIX = "gemfire.default.";

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
