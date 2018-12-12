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
package org.apache.geode.internal;

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
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.util.ArgumentRedactor;

/**
 * Utility class to print banner information at manager startup.
 */
public class Banner {

  private Banner() {
    // everything is static so don't allow instance creation
  }

  private static void prettyPrintPath(String path, PrintWriter out) {
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
  static void print(PrintWriter out, String args[]) {
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

    final String SEPARATOR =
        "---------------------------------------------------------------------------";
    int processId = attemptToReadProcessId();
    short currentOrdinal = Version.CURRENT_ORDINAL;

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
    GemFireVersion.print(out);
    out.println("Communications version: " + currentOrdinal);
    out.println("Process ID: " + processId);
    out.println("User: " + userName);
    out.println("Current dir: " + userDir);
    out.println("Home dir: " + userHome);

    if (!commandLineArguments.isEmpty()) {
      out.println("Command Line Parameters:");
      for (String arg : commandLineArguments) {
        out.println("  " + ArgumentRedactor.redact(arg));
      }
    }

    out.println("Class Path:");
    prettyPrintPath((String) javaClassPath, out);
    out.println("Library Path:");
    prettyPrintPath((String) javaLibraryPath, out);

    if (Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "disableSystemPropertyLogging")) {
      out.println("System property logging disabled.");
    } else {
      out.println("System Properties:");
      for (Object o : sp.entrySet()) {
        Entry me = (Entry) o;
        String key = me.getKey().toString();
        String value =
            ArgumentRedactor.redactArgumentIfNecessary(key, String.valueOf(me.getValue()));
        out.println("    " + key + " = " + value);
      }
      out.println("Log4J 2 Configuration:");
      out.println("    " + LogService.getConfigurationInfo());
    }
    out.println(SEPARATOR);
  }

  /**
   * @return The PID of the current process, or -1 if the PID cannot be determined.
   */
  private static int attemptToReadProcessId() {
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

  private static void printASFLicense(PrintWriter out) {
    out.println("  ");
    out.println("  Licensed to the Apache Software Foundation (ASF) under one or more");
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
   * Return a string containing the banner information.
   *
   * @param args possibly null list of command line arguments
   */
  public static String getString(String args[]) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    print(pw, args);
    pw.close();
    return sw.toString();
  }
}
