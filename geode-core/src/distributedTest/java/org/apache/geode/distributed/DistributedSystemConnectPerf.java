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
package org.apache.geode.distributed;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;

import java.io.PrintStream;
import java.util.Properties;

import org.apache.geode.internal.ExitCode;

/**
 * This program is used to measure the amount of time it takes to connect and re-connect to a
 * {@link DistributedSystem}.
 */
public class DistributedSystemConnectPerf {

  private static final PrintStream out = System.out;
  private static final PrintStream err = System.err;

  private static void usage(String s) {
    err.println("\n** " + s + "\n");
    err.println("usage: java DistributedSystemConnectPerf " + "[options] port iterations");
    err.println("  port          Port on which locator runs");
    err.println("  iterations    Number of times to " + "connect/disconnect");
    err.println("Where options are:");
    err.println("  -wait time    Time (in milliseconds) connection " + "is open");
    err.println();
    err.println("This program measures the amount of time it takes "
        + "to connect/disconnect to a DistributedSystem");
    err.println();

    ExitCode.FATAL.doSystemExit();
  }

  public static void main(String[] args) throws Exception {
    int port = -1;
    int iterations = -1;
    int wait = 0;

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-wait")) {
        if (++i >= args.length) {
          usage("Missing wait time");

        } else {
          try {
            wait = Integer.parseInt(args[i]);

          } catch (NumberFormatException ex) {
            usage("Malformed wait time: " + args[i]);
          }
        }

      } else if (port == -1) {
        try {
          port = Integer.parseInt(args[i]);

        } catch (NumberFormatException ex) {
          usage("Malformed port: " + args[i]);
        }

      } else if (iterations == -1) {
        try {
          iterations = Integer.parseInt(args[i]);

        } catch (NumberFormatException ex) {
          usage("Malformed iterations: " + args[i]);
        }

      } else {
        usage("Extraneous command line: " + args[i]);
      }
    }

    if (port == -1) {
      usage("Missing port");

    } else if (iterations == -1) {
      usage("Missing iterations");
    }

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOG_LEVEL, "info");
    props.setProperty(LOCATORS, "localhost[" + port + "]");

    long totalConnect = 0;
    long totalDisconnect = 0;

    for (int i = 0; i < iterations; i++) {
      long start = System.currentTimeMillis();
      DistributedSystem system = DistributedSystem.connect(props);
      long delta = System.currentTimeMillis() - start;
      totalConnect += delta;
      out.println("** Connected to DistributedSystem " + "(took " + delta + " ms)");

      Thread.sleep(wait);

      start = System.currentTimeMillis();
      system.disconnect();
      delta = System.currentTimeMillis() - start;
      totalDisconnect += delta;
      out.println("** Disconnected from DistributedSystem " + "(took " + delta + " ms)");

    }

    out.println("** Average connect time took: " + (totalConnect / iterations) + " ms");
    out.println("** Average disconnect time took: " + (totalDisconnect / iterations) + " ms");

    ExitCode.NORMAL.doSystemExit();
  }

}
