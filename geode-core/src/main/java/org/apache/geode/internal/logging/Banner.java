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
package org.apache.geode.internal.logging;

import static org.apache.geode.internal.logging.Banner.BannerHeader.CLASS_PATH;
import static org.apache.geode.internal.logging.Banner.BannerHeader.COMMAND_LINE_PARAMETERS;
import static org.apache.geode.internal.logging.Banner.BannerHeader.COMMUNICATIONS_VERSION;
import static org.apache.geode.internal.logging.Banner.BannerHeader.CURRENT_DIR;
import static org.apache.geode.internal.logging.Banner.BannerHeader.HOME_DIR;
import static org.apache.geode.internal.logging.Banner.BannerHeader.LIBRARY_PATH;
import static org.apache.geode.internal.logging.Banner.BannerHeader.LICENSE_START;
import static org.apache.geode.internal.logging.Banner.BannerHeader.LOG4J2_CONFIGURATION;
import static org.apache.geode.internal.logging.Banner.BannerHeader.PROCESS_ID;
import static org.apache.geode.internal.logging.Banner.BannerHeader.SYSTEM_PROPERTIES;
import static org.apache.geode.internal.logging.Banner.BannerHeader.USER;
import static org.apache.geode.internal.util.ProductVersionUtil.getFullVersion;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.geode.SystemFailure;
import org.apache.geode.internal.SystemDescription;
import org.apache.geode.internal.VersionDescription;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.util.ArgumentRedactor;
import org.apache.geode.logging.internal.ConfigurationInfo;
import org.apache.geode.logging.internal.OSProcess;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Utility class to print banner information at manager startup.
 *
 * <p>
 * Banner should only be accessed from the Logging package or from Logging tests.
 */
public class Banner {

  public static final String SEPARATOR =
      "---------------------------------------------------------------------------";

  private final String configurationInfo;

  /**
   * @deprecated Please use {@link Banner(String)} instead. Banner should only be accessed from the
   *             Logging package or from Logging tests.
   */
  @Deprecated
  public Banner() {
    this(ConfigurationInfo.getConfigurationInfo());
  }

  public Banner(final String configurationInfo) {
    this.configurationInfo = configurationInfo;
  }

  public String getString() {
    return getString(null);
  }

  /**
   * Return a string containing the banner information.
   *
   * @param args possibly null list of command line arguments
   */
  public String getString(String[] args) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    print(pw, args);
    pw.close();
    return sw.toString();
  }

  private void prettyPrintPath(String path, PrintWriter out) {
    if (path != null) {
      StringTokenizer st = new StringTokenizer(path, System.getProperty("path.separator"));
      while (st.hasMoreTokens()) {
        out.println("  " + st.nextToken());
      }
    }
  }

  /**
   * Print information about this process to the specified stream.
   *
   * @param args possibly null list of command line arguments
   */
  private void print(PrintWriter out, String args[]) {
    // Copy the system properties for printing. Some are given explicit lines, and
    // others are suppressed. Remove these keys, keeping those we want.
    Map sp = new TreeMap((Properties) System.getProperties().clone()); // fix for 46822
    Object userName = sp.get("user.name");
    Object userDir = sp.get("user.dir");
    Object userHome = sp.get("user.home");
    Object javaClassPath = sp.get("java.class.path");
    Object javaLibraryPath = sp.get("java.library.path");
    sp.remove("user.name");
    sp.remove("user.dir");
    sp.remove("user.home");
    sp.remove("java.class.path");
    sp.remove("java.library.path");
    sp.remove("os.name");
    sp.remove("os.arch");

    int processId = attemptToReadProcessId();
    short currentOrdinal = KnownVersion.CURRENT_ORDINAL;

    List<String> commandLineArguments = new ArrayList<>();
    RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
    if (runtimeBean != null) {
      // each argument is redacted below.
      commandLineArguments.addAll(runtimeBean.getInputArguments());
    }

    if (args != null && args.length != 0) {
      Collections.addAll(commandLineArguments, args);
    }

    // Print it all out.
    out.println();
    out.println(SEPARATOR);
    printASFLicense(out);
    out.println(SEPARATOR);
    out.println(getFullVersion());
    out.println(SystemDescription.RUNNING_ON + ": " + SystemDescription.getRunningOnInfo());
    out.println(SEPARATOR);
    out.println(COMMUNICATIONS_VERSION.displayValue() + ": " + currentOrdinal);
    out.println(PROCESS_ID.displayValue() + ": " + processId);
    out.println(USER.displayValue() + ": " + userName);
    out.println(CURRENT_DIR.displayValue() + ": " + userDir);
    out.println(HOME_DIR.displayValue() + ": " + userHome);

    if (!commandLineArguments.isEmpty()) {
      out.println(COMMAND_LINE_PARAMETERS.displayValue() + ":");
      for (String arg : commandLineArguments) {
        out.println("  " + ArgumentRedactor.redact(arg));
      }
    }

    out.println(CLASS_PATH.displayValue() + ":");
    prettyPrintPath((String) javaClassPath, out);
    out.println(LIBRARY_PATH.displayValue() + ":");
    prettyPrintPath((String) javaLibraryPath, out);

    if (Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "disableSystemPropertyLogging")) {
      out.println("System property logging disabled.");
    } else {
      out.println(SYSTEM_PROPERTIES.displayValue() + ":");
      for (Object o : sp.entrySet()) {
        Entry me = (Entry) o;
        String key = me.getKey().toString();
        String value =
            ArgumentRedactor.redactArgumentIfNecessary(key, String.valueOf(me.getValue()));
        out.println("    " + key + " = " + value);
      }
      out.println(LOG4J2_CONFIGURATION.displayValue() + ":");
      out.println("    " + configurationInfo);
    }
    out.println(SEPARATOR);
  }

  /**
   * @return The PID of the current process, or -1 if the PID cannot be determined.
   */
  private int attemptToReadProcessId() {
    int processId = -1;
    try {
      processId = OSProcess.getId();
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Throwable t) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
    }
    return processId;
  }

  private void printASFLicense(PrintWriter out) {
    out.println("  ");
    out.println("  " + LICENSE_START.displayValue());
    out.println("  contributor license agreements.  See the NOTICE file distributed with this");
    out.println("  work for additional information regarding copyright ownership.");
    out.println("   ");
    out.println("  The ASF licenses this file to You under the Apache License, Version 2.0");
    out.println("  (the \"License\"); you may not use this file except in compliance with the");
    out.println("  License.  You may obtain a copy of the License at");
    out.println("  ");
    out.println("  http://www.apache.org/licenses/LICENSE-2.0");
    out.println("  ");
    out.println("  Unless required by applicable law or agreed to in writing, software");
    out.println("  distributed under the License is distributed on an \"AS IS\" BASIS, WITHOUT");
    out.println("  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the");
    out.println("  License for the specific language governing permissions and limitations");
    out.println("  under the License.");
    out.println("  ");
  }

  /**
   * The headers of the log {@link Banner}.
   */
  public enum BannerHeader {

    LICENSE_START("Licensed to the Apache Software Foundation (ASF) under one or more"),
    BUILD_ID(VersionDescription.BUILD_ID),
    BUILD_JAVA_VERSION(VersionDescription.BUILD_JAVA_VERSION),
    BUILD_PLATFORM(VersionDescription.BUILD_PLATFORM),
    PRODUCT_NAME(VersionDescription.PRODUCT_NAME),
    PRODUCT_VERSION(VersionDescription.PRODUCT_VERSION),
    SOURCE_DATE(VersionDescription.SOURCE_DATE),
    SOURCE_REPOSITORY(VersionDescription.SOURCE_REPOSITORY),
    SOURCE_REVISION(VersionDescription.SOURCE_REVISION),
    RUNNING_ON(SystemDescription.RUNNING_ON),
    COMMUNICATIONS_VERSION("Communications version"),
    PROCESS_ID("Process ID"),
    USER("User"),
    CURRENT_DIR("Current dir"),
    HOME_DIR("Home dir"),
    COMMAND_LINE_PARAMETERS("Command Line Parameters"),
    CLASS_PATH("Class Path"),
    LIBRARY_PATH("Library Path"),
    SYSTEM_PROPERTIES("System Properties"),
    LOG4J2_CONFIGURATION("Log4J 2 Configuration");

    private final String displayValue;

    BannerHeader(String displayValue) {
      this.displayValue = displayValue;
    }

    public String displayValue() {
      return displayValue;
    }

    public static String[] displayValues() {
      String[] headerValues = new String[BannerHeader.values().length];
      int i = 0;
      for (BannerHeader bannerHeader : BannerHeader.values()) {
        headerValues[i] = bannerHeader.displayValue();
        i++;
      }
      return headerValues;
    }
  }
}
