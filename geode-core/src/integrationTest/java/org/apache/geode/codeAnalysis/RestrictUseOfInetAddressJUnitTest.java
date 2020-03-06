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
package org.apache.geode.codeAnalysis;


import static org.assertj.core.api.Assertions.assertThat;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.codeAnalysis.decode.CompiledClass;
import org.apache.geode.test.junit.rules.ClassAnalysisRule;

public class RestrictUseOfInetAddressJUnitTest {

  @Rule
  public ClassAnalysisRule classProvider = new ClassAnalysisRule(getModuleName());


  public String getModuleName() {
    return "geode-core";
  }

  @AfterClass
  public static void afterClass() {
    ClassAnalysisRule.clearCache();
  }

  /**
   * Use of class InetAddress is restricted. We do not want to resolve host names
   * until the time we form a tcp/ip connection since the host's ip address may
   * change, and also to support proxies providing access to cloud-based clusters.
   */
  @Test
  public void restrictUseOfInetAddressToSanctionedClasses() {
    Map<String, CompiledClass> classes = classProvider.getClasses();
    List<String> exclusions = getSanctionedUsersOfInetAddress();

    System.out.println("ignoring " + exclusions.size() + " sanctioned classes");
    for (String exclusion : exclusions) {
      CompiledClass removedClass = classes.remove(exclusion);
      assertThat(removedClass)
          .withFailMessage("test needs to be updated as " + exclusion + " could not be found")
          .isNotNull();
    }

    final String inetAddressName = "java/net/InetAddress";
    StringWriter writer = new StringWriter();
    int failures = 0;

    System.out.println("Looking for unsanctioned uses of InetAddress in " + getModuleName());
    final List<String> keys = new ArrayList<>(classes.keySet());
    Collections.sort(keys);
    for (String className : keys) {
      if (classes.get(className).refersToClass(inetAddressName)) {
        if (failures > 0) {
          writer.append(",\n");
        }
        writer.append('"').append(className).append('"');
        failures++;
      }
    }

    // use an assertion on the StringWriter rather than the failure count so folks can
    // tell what failed
    String actual = writer.toString();
    String expected = "";
    assertThat(actual)
        .withFailMessage("Unexpected use of InetAddress needs to be discussed.\n"
            + "The actual use may be hidden in the code, such as LocalHostUtil.getLocalHost().getHostName().\n"
            + "Use of InetAddress can cause off-platform errors.\n"
            + "Classes detected but not sanctioned are:\n" + actual)
        .isEqualTo(expected);
  }

  @Test
  public void restrictUseOfInetSocketAddress_getAddress() {
    Map<String, CompiledClass> classes = classProvider.getClasses();
    List<String> exclusions = Arrays.asList(
        // server-side bind address
        "org/apache/geode/admin/internal/DistributionLocatorImpl",
        // server-side bind address
        "org/apache/geode/internal/cache/tier/sockets/AcceptorImpl",
        // allowed in both client and server
        "org/apache/geode/internal/net/SCAdvancedSocketCreator",
        // server side communications
        "org/apache/geode/internal/tcp/Connection");

    System.out.println("ignoring " + exclusions.size() + " sanctioned classes");
    for (String exclusion : exclusions) {
      CompiledClass removedClass = classes.remove(exclusion);
      assertThat(removedClass)
          .withFailMessage("test needs to be updated as " + exclusion + " could not be found")
          .isNotNull();
    }

    final String inetSocketAddress = "java/net/InetSocketAddress";
    final String getAddress = "getAddress";
    StringWriter writer = new StringWriter();
    int failures = 0;

    System.out.println(
        "Looking for unsanctioned uses of InetSocketAddress.getAddress() in " + getModuleName());
    final List<String> keys = new ArrayList<>(classes.keySet());
    Collections.sort(keys);
    for (String className : keys) {
      if (classes.get(className).refersToMethod(inetSocketAddress, getAddress)) {
        if (failures > 0) {
          writer.append(",\n");
        }
        writer.append('"').append(className).append('"');
        failures++;
      }
    }

    // use an assertion on the StringWriter rather than the failure count so folks can
    // tell what failed
    String actual = writer.toString();
    String expected = "";
    assertThat(actual)
        .withFailMessage("Unexpected use of InetSocketAddress.getAddress() needs to be discussed.\n"
            + "Use of InetSocketAddress.getAddress() can cause off-platform errors.\n"
            + "Classes detected but not sanctioned are:\n" + actual)
        .isEqualTo(expected);
  }

  private List<String> getSanctionedUsersOfInetAddress() {
    return Arrays.asList(
        // serialization of InetAddresses - does not resolve host names
        "org/apache/geode/DataSerializer",
        "org/apache/geode/internal/InternalDataSerializer$4",
        "org/apache/geode/internal/InternalDataSerializer$5",
        "org/apache/geode/internal/InternalDataSerializer$6",

        // deprecated admin API
        "org/apache/geode/admin/GemFireMemberStatus", // InetAddress used in public API
        "org/apache/geode/admin/internal/AdminDistributedSystemImpl",
        "org/apache/geode/admin/internal/ConfigurationParameterImpl",
        "org/apache/geode/admin/internal/DistributionLocatorConfigImpl",
        "org/apache/geode/admin/internal/DistributionLocatorImpl",
        "org/apache/geode/admin/internal/DistributedSystemHealthMonitor",
        "org/apache/geode/admin/internal/GemFireHealthImpl",
        "org/apache/geode/admin/internal/InetAddressUtils",
        "org/apache/geode/admin/internal/InetAddressUtilsWithLogging",
        "org/apache/geode/admin/internal/ManagedEntityConfigImpl",
        "org/apache/geode/admin/internal/SystemMemberImpl",
        "org/apache/geode/admin/jmx/internal/AdminDistributedSystemJmxImpl",
        "org/apache/geode/admin/jmx/internal/AgentConfigImpl",
        "org/apache/geode/admin/jmx/internal/MemberInfoWithStatsMBean",
        "org/apache/geode/admin/jmx/internal/RMIServerSocketFactoryImpl",
        "org/apache/geode/internal/admin/remote/DistributionLocatorId",
        "org/apache/geode/internal/admin/remote/FetchHostResponse",
        "org/apache/geode/internal/admin/remote/RemoteGemFireVM",

        // server-side locator launcher
        "org/apache/geode/distributed/AbstractLauncher",
        "org/apache/geode/distributed/Locator",
        "org/apache/geode/distributed/LocatorLauncher",
        "org/apache/geode/distributed/LocatorLauncher$Builder",
        "org/apache/geode/distributed/LocatorLauncher$LocatorState",

        // server-side cache-server launcher
        "org/apache/geode/distributed/ServerLauncher",
        "org/apache/geode/distributed/ServerLauncher$Builder",
        "org/apache/geode/distributed/ServerLauncher$ServerState",

        // peer-to-peer locator and multicast address lookup
        "org/apache/geode/distributed/internal/AbstractDistributionConfig",
        "org/apache/geode/distributed/internal/DistributionConfigImpl",
        "org/apache/geode/internal/AbstractConfig",

        // server-side message distribution
        "org/apache/geode/distributed/internal/ClusterDistributionManager",
        "org/apache/geode/distributed/internal/InternalDistributedSystem",
        "org/apache/geode/distributed/internal/StartupMessage",
        "org/apache/geode/distributed/internal/direct/DirectChannel",
        "org/apache/geode/distributed/internal/membership/InternalDistributedMember",
        "org/apache/geode/distributed/internal/membership/adapter/ServiceConfig",
        "org/apache/geode/internal/tcp/ConnectionTable",
        "org/apache/geode/internal/tcp/ConnectionTable$ConnectingSocketInfo",
        "org/apache/geode/internal/tcp/TCPConduit",

        // server-side location service
        "org/apache/geode/distributed/internal/InternalLocator",
        "org/apache/geode/distributed/internal/ServerLocation",
        "org/apache/geode/distributed/internal/ServerLocator",

        // local address lookup used by launchers and tests
        "org/apache/geode/internal/AvailablePort",

        // old main() method used by deprecated main() method in Locator
        "org/apache/geode/internal/DistributionLocator",

        // old, unused command-line interface replaced by gfsh
        "org/apache/geode/internal/SystemAdmin",
        "org/apache/geode/internal/ManagerInfo",

        // persistent IDs - is this a problem if client uses persistence?
        "org/apache/geode/internal/cache/partitioned/PersistentBucketRecoverer$RegionStatus",
        "org/apache/geode/internal/cache/persistence/PersistentMemberID",
        "org/apache/geode/internal/cache/persistence/PersistentMemberPattern",
        "org/apache/geode/internal/cache/snapshot/ParallelSnapshotFileMapper",

        // server-side client/server messaging
        "org/apache/geode/internal/cache/tier/sockets/AcceptorImpl",
        "org/apache/geode/internal/cache/tier/sockets/CacheClientProxy",
        "org/apache/geode/internal/cache/tier/sockets/CacheClientUpdater",
        "org/apache/geode/internal/cache/tier/sockets/ClientHealthMonitor",
        "org/apache/geode/internal/cache/tier/sockets/ServerConnection",
        "org/apache/geode/internal/cache/IncomingGatewayStatus",

        // socket-creator is allowed to use InetAddress in clients and in servers
        "org/apache/geode/internal/net/SCAdvancedSocketCreator",
        "org/apache/geode/internal/net/SocketCreator",

        // code used only by persistence but in "org.apache.geode.internal.util" package
        "org/apache/geode/internal/util/TransformUtils$2",

        // management API
        "org/apache/geode/management/internal/JmxManagerAdvisee",
        "org/apache/geode/management/internal/JmxManagerLocatorResponse",
        "org/apache/geode/management/internal/MBeanJMXAdapter",
        "org/apache/geode/management/internal/ManagementAgent",
        "org/apache/geode/management/internal/RestAgent",
        "org/apache/geode/management/internal/beans/BeanUtilFuncs",
        "org/apache/geode/management/internal/beans/DistributedSystemBridge",
        "org/apache/geode/management/internal/beans/LocatorMBeanBridge",
        "org/apache/geode/management/internal/configuration/handlers/ClusterManagementServiceInfoRequestHandler",
        "org/apache/geode/management/internal/util/HostUtils"

    );
  }
}
