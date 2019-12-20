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

import static org.apache.commons.lang3.Validate.notNull;

import java.util.Properties;

import org.apache.geode.distributed.internal.DistributionConfig;

/**
 * Thread based context for launching a process. GemFire internals can acquire optional
 * configuration details from a process launcher via this context.
 *
 * @since GemFire 7.0
 */
public class ProcessLauncherContext {

  public static final String OVERRIDDEN_DEFAULTS_PREFIX =
      DistributionConfig.GEMFIRE_PREFIX + "default.";

  /**
   * Default value for {@link #isRedirectingOutput()}
   */
  private static final boolean REDIRECT_OUTPUT_DEFAULT = false;

  private static final ThreadLocal<ProcessLauncherContext> DATA = new ThreadLocal<>();

  private final boolean redirectOutput;
  private final Properties overriddenDefaults;
  private final StartupStatusListener startupListener;

  private static ProcessLauncherContext get() {
    return DATA.get();
  }

  /**
   * Returns true if this process should redirect output to the system log.
   *
   * @return true if this process should redirect output to the system log
   */
  public static boolean isRedirectingOutput() {
    ProcessLauncherContext context = get();
    if (context == null) {
      return REDIRECT_OUTPUT_DEFAULT;
    }
    return context.redirectOutput();
  }

  /**
   * Returns the gemfire properties to be used if none of the specified properties are defined by
   * any other mechanism. This will only override default values. If a property is defined by System
   * property, API, or within gemfire.properties then the contingent value will be ignored.
   *
   * @return the contingent gemfire properties values to be used as an alternative default value
   */
  public static Properties getOverriddenDefaults() {
    ProcessLauncherContext context = get();
    if (context == null) {
      return new Properties();
    }
    return context.overriddenDefaults();
  }

  public static StartupStatusListener getStartupListener() {
    ProcessLauncherContext context = get();
    if (context == null) {
      return null;
    }

    return context.startupListener();
  }

  /**
   * Sets the ProcessLauncherContext data for the calling thread.
   */
  public static void set(final boolean redirectOutput, final Properties overriddenDefaults,
      final StartupStatusListener startupListener) {
    notNull(overriddenDefaults,
        "Invalid overriddenDefaults '" + overriddenDefaults + "' specified");

    DATA.set(new ProcessLauncherContext(redirectOutput, overriddenDefaults, startupListener));
    installLogListener(startupListener);
  }

  /**
   * Clears the current ProcessLauncherContext for the calling thread.
   */
  public static void remove() {
    DATA.remove();
    clearLogListener();
  }

  private static void installLogListener(final StartupStatusListener startupListener) {
    if (startupListener != null) {
      StartupStatusListenerRegistry.setListener(startupListener);
    }
  }

  private static void clearLogListener() {
    StartupStatusListenerRegistry.clearListener();
  }

  private ProcessLauncherContext(final boolean redirectOutput, final Properties overriddenDefaults,
      final StartupStatusListener startupListener) {
    this.redirectOutput = redirectOutput;
    this.overriddenDefaults = overriddenDefaults;
    this.startupListener = startupListener;
  }

  private boolean redirectOutput() {
    return redirectOutput;
  }

  private Properties overriddenDefaults() {
    return overriddenDefaults;
  }

  private StartupStatusListener startupListener() {
    return startupListener;
  }
}
