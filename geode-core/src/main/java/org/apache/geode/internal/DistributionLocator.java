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


import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;

import org.apache.logging.log4j.Logger;

import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * This class is used to work with a managed VM that hosts a
 * {@link org.apache.geode.distributed.Locator}.
 *
 * @since GemFire 2.0
 */
public class DistributionLocator {

  private static final Logger logger = LogService.getLogger();

  public static final String TEST_OVERRIDE_DEFAULT_PORT_PROPERTY =
      GeodeGlossary.GEMFIRE_PREFIX + "test.DistributionLocator.OVERRIDE_DEFAULT_PORT";

  /** Default file name for locator log: <code>"locator.log"</code> */
  public static final String DEFAULT_LOG_FILE = "locator.log";
  public static final String DEFAULT_STARTUP_LOG_FILE = "start_locator.log";

  public static final int DEFAULT_LOCATOR_PORT = 10334;
  public static final boolean LOAD_SHARED_CONFIGURATION = false;

  public static int parsePort(String portOption) {
    if (portOption == null || portOption.equals("")) {
      return Integer.getInteger(TEST_OVERRIDE_DEFAULT_PORT_PROPERTY, DEFAULT_LOCATOR_PORT);
    } else {
      int result = Integer.parseInt(portOption);
      if (result < 1 || result > 65535) {
        throw new IllegalArgumentException(
            "The -port= argument must be greater than 0 and less than 65536.");
      } else {
        return result;
      }
    }
  }

  @MakeNotStatic
  private static boolean shutdown = false;
  @MakeNotStatic
  private static File lockFile = null;
  private static final File directory = (new File("")).getAbsoluteFile();

  /**
   * Cleans up the artifacts left by this managed locator VM.
   */
  protected static void shutdown(int port, InetAddress address) throws IOException {
    if (shutdown) {
      return;
    }
    shutdown = true;
    if (directory != null) {
      ManagerInfo.setLocatorStopping(directory, port, address);
    }
    if (lockFile != null) {
      if (!lockFile.delete() && lockFile.exists()) {
        IOException e = new IOException("Unable to delete " + lockFile.getAbsolutePath());
        e.printStackTrace(); // What else to do?
      }
    }
    logger.info("Locator stopped");
  }

  public static void main(String args[]) {
    if (args.length == 0 || args.length > 6) {
      System.err.println(
          "Usage: port [bind-address] [peerLocator] [serverLocator] [hostname-for-clients]");
      System.err
          .println("A zero-length address will bind to localhost");
      System.err.println(
          "A zero-length gemfire-properties-file will mean use the default search path");
      System.err.println(
          "peerLocator and serverLocator both default to true");
      System.err.println(
          "A zero-length hostname-for-clients will default to bind-address");
      ExitCode.FATAL.doSystemExit();
    } else {
      shutdown = false;
    }
    SystemFailure.loadEmergencyClasses();

    final int port = parsePort(args[0]);
    InetAddress address = null;
    boolean peerLocator = true;
    boolean serverLocator = true;
    String hostnameForClients = null;
    try {
      if (args.length > 1 && !args[1].equals("")) {
        if (!SystemAdmin.validLocalAddress(args[1])) {
          System.err.println(
              String.format("'%s' is not a valid IP address for this machine",
                  args[1]));
          ExitCode.FATAL.doSystemExit();
        }
        address = InetAddress.getByName(args[1]);
      } else {
        // address = null; // was InetAddress.getLocalHost(); (redundant assignment)
      }
      if (args.length > 2) {
        peerLocator = "true".equalsIgnoreCase(args[2]);
      }
      if (args.length > 3) {
        serverLocator = "true".equalsIgnoreCase(args[3]);
      }
      if (args.length > 4) {
        hostnameForClients = args[4];
      }

      if (!Boolean.getBoolean(InternalDistributedSystem.DISABLE_SHUTDOWN_HOOK_PROPERTY)) {
        final InetAddress faddress = address;
        Runtime.getRuntime()
            .addShutdownHook(new LoggingThread("LocatorShutdownThread", false, () -> {
              try {
                DistributionLocator.shutdown(port, faddress);
              } catch (IOException e) {
                e.printStackTrace();
              }
            }));
      }

      lockFile = ManagerInfo.setLocatorStarting(directory, port, address);
      lockFile.deleteOnExit();

      try {

        InternalLocator locator = InternalLocator.startLocator(port, new File(DEFAULT_LOG_FILE),
            null, null, address, true, (Properties) null, hostnameForClients);

        ManagerInfo.setLocatorStarted(directory, port, address);
        locator.waitToStop();

      } finally {
        shutdown(port, address);
      }

    } catch (InterruptedException ex) {
      // We were interrupted while waiting for the locator to stop.
      // No big deal.

    } catch (java.net.BindException ex) {
      logger.fatal("Could not bind locator to {}[{}]",
          new Object[] {address, port});
      ExitCode.FATAL.doSystemExit();

    } catch (Exception ex) {
      logger.fatal("Could not start locator",
          ex);
      ExitCode.FATAL.doSystemExit();
    }
  }

}
