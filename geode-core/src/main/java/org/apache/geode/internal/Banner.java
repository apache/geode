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

import org.apache.geode.SystemFailure;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.util.ArgumentRedactor;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.TreeMap;

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
    Map sp = new TreeMap((Properties) System.getProperties().clone()); // fix for 46822
    int processId = -1;
    final String SEPARATOR =
        "---------------------------------------------------------------------------";
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
    out.println();

    final String productName = GemFireVersion.getProductName();

    out.println(SEPARATOR);

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

    out.println(SEPARATOR);

    GemFireVersion.print(out);

    out.println("Communications version: " + Version.CURRENT_ORDINAL);

    out.println("Process ID: " + processId);
    out.println("User: " + sp.get("user.name"));
    sp.remove("user.name");
    sp.remove("os.name");
    sp.remove("os.arch");
    out.println("Current dir: " + sp.get("user.dir"));
    sp.remove("user.dir");
    out.println("Home dir: " + sp.get("user.home"));
    sp.remove("user.home");
    List<String> allArgs = new ArrayList<>();
    {
      RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
      if (runtimeBean != null) {
        allArgs.addAll(runtimeBean.getInputArguments()); // fixes 45353
      }
    }

    if (args != null && args.length != 0) {
      Collections.addAll(allArgs, args);
    }
    if (!allArgs.isEmpty()) {
      out.println("Command Line Parameters:");
      for (String arg : allArgs) {
        out.println("  " + ArgumentRedactor.redact(arg));
      }
    }

    out.println("Class Path:");
    prettyPrintPath((String) sp.get("java.class.path"), out);
    sp.remove("java.class.path");
    out.println("Library Path:");
    prettyPrintPath((String) sp.get("java.library.path"), out);
    sp.remove("java.library.path");

    if (Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "disableSystemPropertyLogging")) {
      out.println("System property logging disabled.");
    } else {
      out.println("System Properties:");
      Iterator it = sp.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry me = (Map.Entry) it.next();
        String key = me.getKey().toString();
        out.println("    " + key + " = " + ArgumentRedactor.redact(String.valueOf(me.getValue())));
      }
      out.println("Log4J 2 Configuration:");
      out.println("    " + LogService.getConfigInformation());
    }
    out.println(SEPARATOR);
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
