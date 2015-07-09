/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import java.io.*;
import java.util.*;

/**
 * This program is used to measure the amount of time it takes to
 * connect and re-connect to a {@link DistributedSystem}.
 */
public class DistributedSystemConnectPerf {

  private static final PrintStream out = System.out;
  private static final PrintStream err = System.err;

  private static void usage(String s) {
    err.println("\n** " + s + "\n");
    err.println("usage: java DistributedSystemConnectPerf " +
                "[options] port iterations");
    err.println("  port          Port on which locator runs");
    err.println("  iterations    Number of times to " +
                "connect/disconnect");
    err.println("Where options are:");
    err.println("  -wait time    Time (in milliseconds) connection " +
                "is open");
    err.println("");
    err.println("This program measures the amount of time it takes " +
                "to connect/disconnect to a DistributedSystem");
    err.println("");

    System.exit(1);
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

    System.setProperty("DistributionManager.VERBOSE", "true");

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, "info");
    props.setProperty(DistributionConfig.LOCATORS_NAME,
                      "localhost[" + port + "]");

    long totalConnect = 0;
    long totalDisconnect = 0;

    for (int i = 0; i < iterations; i++) {
      long start = System.currentTimeMillis();
      DistributedSystem system = DistributedSystem.connect(props);
      long delta = System.currentTimeMillis() - start;
      totalConnect += delta;
      out.println("** Connected to DistributedSystem " +
                                 "(took " + delta + " ms)");

      Thread.sleep(wait);

      start = System.currentTimeMillis();
      system.disconnect();
      delta = System.currentTimeMillis() - start;
      totalDisconnect += delta;
      out.println("** Disconnected from DistributedSystem " +
                  "(took " + delta + " ms)");

    }

    out.println("** Average connect time took: " +
                (totalConnect / iterations) + " ms");
    out.println("** Average disconnect time took: " +
                (totalDisconnect / iterations) + " ms");

    System.exit(0);
  }

}
