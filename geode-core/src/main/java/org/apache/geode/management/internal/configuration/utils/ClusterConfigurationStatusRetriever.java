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
package org.apache.geode.management.internal.configuration.utils;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;
import java.util.Set;

import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.internal.cache.persistence.PersistentMemberPattern;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.configuration.messages.SharedConfigurationStatusRequest;
import org.apache.geode.management.internal.configuration.messages.SharedConfigurationStatusResponse;

public class ClusterConfigurationStatusRetriever {
  private static final int NUM_ATTEMPTS_FOR_SHARED_CONFIGURATION_STATUS = 3;

  public static String fromLocator(String locatorHostName, int locatorPort, Properties configProps)
      throws ClassNotFoundException, IOException {
    final StringBuilder buffer = new StringBuilder();

    try {
      final InetAddress networkAddress = InetAddress.getByName(locatorHostName);

      TcpClient client = new TcpClient(configProps);
      SharedConfigurationStatusResponse statusResponse =
          (SharedConfigurationStatusResponse) client.requestToServer(networkAddress, locatorPort,
              new SharedConfigurationStatusRequest(), 10000, true);



      for (int i = 0; i < NUM_ATTEMPTS_FOR_SHARED_CONFIGURATION_STATUS; i++) {
        if (statusResponse.getStatus().equals(
            org.apache.geode.management.internal.configuration.domain.SharedConfigurationStatus.STARTED)
            || statusResponse.getStatus().equals(
                org.apache.geode.management.internal.configuration.domain.SharedConfigurationStatus.NOT_STARTED)) {
          statusResponse =
              (SharedConfigurationStatusResponse) client.requestToServer(networkAddress,
                  locatorPort, new SharedConfigurationStatusRequest(), 10000, true);
          try {
            Thread.sleep(5000);
          } catch (InterruptedException e) {
            // Swallow the exception
          }
        } else {
          break;
        }
      }

      switch (statusResponse.getStatus()) {
        case RUNNING:
          buffer.append("\nCluster configuration service is up and running.");
          break;
        case STOPPED:
          buffer.append(
              "\nCluster configuration service failed to start , please check the log file for errors.");
          break;
        case WAITING:
          buffer.append(
              "\nCluster configuration service is waiting for other locators with newer shared configuration data.");
          Set<PersistentMemberPattern> pmpSet = statusResponse.getOtherLocatorInformation();
          if (!pmpSet.isEmpty()) {
            buffer.append("\nThis locator might have stale cluster configuration data.");
            buffer.append(
                "\nFollowing locators contain potentially newer cluster configuration data");

            for (PersistentMemberPattern pmp : pmpSet) {
              buffer.append("\nHost : ").append(pmp.getHost());
              buffer.append("\nDirectory : ").append(pmp.getDirectory());
            }
          } else {
            buffer.append("\nPlease check the log file for errors");
          }
          break;
        case UNDETERMINED:
          buffer.append(
              "\nUnable to determine the status of shared configuration service, please check the log file");
          break;
        case NOT_STARTED:
          buffer.append("\nCluster configuration service has not been started yet");
          break;
        case STARTED:
          buffer
              .append("\nCluster configuration service has been started, but its not running yet");
          break;
      }
    } catch (Exception e) {
      // TODO fix this once Trac Bug #50513 gets fixed
      // NOTE this ClassCastException occurs if the a plain text TCP/IP connection is used to
      // connect to a Locator
      // configured with SSL.
      Gfsh.getCurrentInstance()
          .logToFile(String.format(
              "Failed to get the status of the Shared Configuration Service running on Locator (%1$s[%2$d])!",
              locatorHostName, locatorPort), e);
    }

    return buffer.toString();
  }

  public static String fromLocator(LocatorLauncher.LocatorState locatorState)
      throws ClassNotFoundException, IOException {
    return fromLocator(locatorState.getHost(), Integer.parseInt(locatorState.getPort()),
        new Properties());
  }
}
