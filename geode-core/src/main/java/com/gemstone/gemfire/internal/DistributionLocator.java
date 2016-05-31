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
package com.gemstone.gemfire.internal;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.util.Properties;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

/**
 * This class is used to work with a managed VM that hosts a {@link
 * com.gemstone.gemfire.distributed.Locator}.
 *
 * @since GemFire 2.0
 */
public class DistributionLocator  {
  
  private static final Logger logger = LogService.getLogger();
  
  public static final String TEST_OVERRIDE_DEFAULT_PORT_PROPERTY = "gemfire.test.DistributionLocator.OVERRIDE_DEFAULT_PORT";

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
        throw new IllegalArgumentException(LocalizedStrings.DistributionLocator_THE_PORT_ARGUMENT_MUST_BE_GREATER_THAN_0_AND_LESS_THAN_65536.toLocalizedString());
      } else {
        return result;
      }
    }
  }

  public static void stop(InetAddress addr, int port) {
    try {
      InternalLocator.stopLocator(port, addr);
    } catch ( ConnectException ignore ) {
      // must not be running
    }
  }
  

  private static boolean shutdown = false;
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
    logger.info(LocalizedStrings.DistributionLocator_LOCATOR_STOPPED);
  }
  
  public static void main(String args[]) {
    if (args.length == 0 || args.length > 6) {
      System.err.println(LocalizedStrings.DistributionLocator_USAGE.toLocalizedString() + ": port [bind-address] [peerLocator] [serverLocator] [hostname-for-clients]");
      System.err.println(LocalizedStrings.DistributionLocator_A_ZEROLENGTH_ADDRESS_WILL_BIND_TO_LOCALHOST.toLocalizedString());
      System.err.println(LocalizedStrings.DistributionLocator_A_ZEROLENGTH_GEMFIREPROPERTIESFILE_WILL_MEAN_USE_THE_DEFAULT_SEARCH_PATH.toLocalizedString());
      System.err.println(LocalizedStrings.DistributionLocator_PEERLOCATOR_AND_SERVERLOCATOR_BOTH_DEFAULT_TO_TRUE.toLocalizedString());
      System.err.println(LocalizedStrings.DistributionLocator_A_ZEROLENGTH_HOSTNAMEFORCLIENTS_WILL_DEFAULT_TO_BINDADDRESS.toLocalizedString());
      System.exit(1);
    } else {
      shutdown = false;
    }
      SystemFailure.loadEmergencyClasses();
      
      //log.info(Banner.getString(args));
      final int port = parsePort(args[0]);
      InetAddress address = null;
      boolean peerLocator  = true;
      boolean serverLocator  = true;
      String hostnameForClients = null;
      try {
        if (args.length > 1 && !args[1].equals("")) {
          if (!SystemAdmin.validLocalAddress(args[1])) {
            System.err.println(LocalizedStrings.DistributionLocator__0_IS_NOT_A_VALID_IP_ADDRESS_FOR_THIS_MACHINE.toLocalizedString(args[1]));
            System.exit(1);
          }
          address = InetAddress.getByName(args[1]);
        } else {
//          address = null; // was InetAddress.getLocalHost(); (redundant assignment)
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

        if ( ! Boolean.getBoolean(InternalDistributedSystem.DISABLE_SHUTDOWN_HOOK_PROPERTY) ) {
          final InetAddress faddress = address;
          Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
              try {
                DistributionLocator.shutdown(port, faddress);
              }
              catch (IOException e) {
                e.printStackTrace();
              }
            }
          }));
        }
          
        lockFile = ManagerInfo.setLocatorStarting(directory, port, address);
        lockFile.deleteOnExit();

        try {
          
          InternalLocator locator = InternalLocator
            .startLocator(port,
                          new File(DEFAULT_LOG_FILE),
                          null,
                          null,
                          null,
                          address,
                          (Properties)null,
                          peerLocator,
                          serverLocator,
                          hostnameForClients, LOAD_SHARED_CONFIGURATION);

          ManagerInfo.setLocatorStarted(directory, port, address);
          locator.waitToStop();

        } finally {
          shutdown(port, address);
        }

      } catch (InterruptedException ex) {
        // We were interrupted while waiting for the locator to stop.
        // No big deal.

      } catch (java.net.BindException ex) {
        logger.fatal(LocalizedMessage.create(LocalizedStrings.DistributionLocator_COULD_NOT_BIND_LOCATOR_TO__0__1, new Object[] {address, Integer.valueOf(port)}));
        System.exit(1);

      } catch (Exception ex) {
        logger.fatal(LocalizedMessage.create(LocalizedStrings.DistributionLocator_COULD_NOT_START_LOCATOR), ex);
        System.exit(1);
      }
  }
 
}
