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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.inet.LocalHostUtil;

/**
 * Represents a distribution locator server that provides discovery information to members and
 * clients of a GemFire distributed system. In most GemFire distributed cache architectures,
 * distribution locators are run in their own process. A stand-alone locator process is managed
 * using gfsh command line utility.
 *
 * The stand-alone locator configuration provides high-availability of membership information.
 *
 * <P>
 *
 * This class allows a GemFire application VM to host a distribution locator. Such a configuration
 * minimizes the number of processes that are required to run GemFire. However, hosting distribution
 * locators is not generally recommended because if the application VM exits it would not be
 * possible for new applications to connect to the distributed system until it is restarted.
 *
 * <P>
 * Locators persist membership information in a locatorXXXview.dat file. This file is used to
 * recover information about the cluster when a locator starts if there are no other currently
 * running locators. It allows the restarted locator to rejoin the cluster.
 *
 * <P>
 * <b>NOTE:</b> In this release of the product locators log membership views and cache server status
 * in a locatorXXXviews.log file, where XXX is the locator's port. This is a rolling log capped in
 * size at 5mb. In order to log cache server status the locator will enable server-location, so the
 * locator must be started with a DistributedSystem or be started so that it creates a
 * DistributedSystem. This means that it is not possible in this release to use APIs to start a
 * locator and <i>then</i> start a DistributedSystem.
 * <P>
 *
 *
 * @since GemFire 4.0
 */
public abstract class Locator {

  ///////////////////// Instance Fields /////////////////////

  /** The file to which this locator logs */
  protected File logFile;

  /** The bind address for this locator */
  protected InetAddress bindAddress;

  /**
   * the hostname to give to clients so they can connect to this locator.
   *
   * @since GemFire 5.7
   */
  protected String hostnameForClients;

  ////////////////////// Static Methods //////////////////////

  /**
   * Starts a new distribution locator host by this VM. The locator's listening sockets will bind to
   * all network addresses. The locator will look for a gemfire.properties file, or equivalent
   * system properties to fill in the gaps in its configuration. If you are using multicast
   * communications, the locator should be configured with the same settings that your applications
   * will use.
   * <p>
   * The locator will not start a distributed system. The locator will provide peer location
   * services only.
   *
   * @param port The port on which the locator will listen for membership information requests from
   *        new members
   * @param logFile The file to which the locator logs information. The directory that contains the
   *        log file is used as the output directory of the locator (see <code>-dir</code> option to
   *        the <code>gemfire</code> command).
   *
   * @throws IllegalArgumentException If <code>port</code> is not in the range 0 to 65536
   * @throws org.apache.geode.SystemIsRunningException If another locator is already running in
   *         <code>outputDir</code>
   * @throws org.apache.geode.GemFireIOException If the directory containing the
   *         <code>logFile</code> does not exist or cannot be written to
   * @throws IOException If the locator cannot be started
   * @deprecated as of 7.0 use startLocatorAndDS instead.
   */
  public static Locator startLocator(int port, File logFile) throws IOException {

    return startLocator(port, logFile, false, (InetAddress) null, (Properties) null, true, true,
        null);
  }

  /**
   * Starts a new distribution locator host by this VM, and an admin distributed system controlled
   * by the locator. The locator's listening sockets will bind to all network addresses. The locator
   * will use the given properties to start an AdminDistributedSystem.
   * <p>
   * The locator starts a AdminDistributedSystem configured with the given properties to provide the
   * system with a long-running process that can be relied on for stable membership information. The
   * locator will provide provide peer and cache server location services.
   *
   * @since GemFire 5.0
   *
   * @param port The port on which the locator will listen for membership information requests from
   *        new members
   * @param logFile The file to which the locator logs information. The directory that contains the
   *        log file is used as the output directory of the locator (see <code>-dir</code> option to
   *        the <code>gemfire</code> command).
   * @param distributedSystemProperties The properties used to configure the locator's distributed
   *        system. If there are multiple locators in the system, this should note them in the
   *        "locators" setting. If The distributed system is using multicast, the "mcast-port"
   *        should also be set.
   *
   * @throws IllegalArgumentException If <code>port</code> is not in the range 0 to 65536
   * @throws org.apache.geode.SystemIsRunningException If another locator is already running in
   *         <code>outputDir</code>
   * @throws org.apache.geode.GemFireIOException If the directory containing the
   *         <code>logFile</code> does not exist or cannot be written to
   * @throws IOException If the locator cannot be started
   */
  public static Locator startLocatorAndDS(int port, File logFile,
      Properties distributedSystemProperties) throws IOException {

    return startLocator(port, logFile, (InetAddress) null, distributedSystemProperties, true, true,
        null);
  }

  /**
   * Starts a new distribution locator host by this VM. The locator will look for a
   * gemfire.properties file, or equivalent system properties to fill in the gaps in its
   * configuration.
   * <p>
   * The locator will not start a distributed system. The locator will provide peer location
   * services only.
   *
   * @param port The port on which the locator will listen for membership information requests from
   *        new members
   * @param logFile The file to which the locator logs information. The directory that contains the
   *        log file is used as the output directory of the locator (see <code>-dir</code> option to
   *        the <code>gemfire</code> command).
   * @param bindAddress The IP address to which the locator's socket binds
   *
   * @throws IllegalArgumentException If <code>port</code> is not in the range 0 to 65536
   * @throws org.apache.geode.SystemIsRunningException If another locator is already running in
   *         <code>outputDir</code>
   * @throws org.apache.geode.GemFireIOException If the directory containing the
   *         <code>logFile</code> does not exist or cannot be written to
   * @throws IOException If the locator cannot be started
   * @deprecated as of 7.0 use startLocatorAndDS instead.
   */
  public static Locator startLocator(int port, File logFile, InetAddress bindAddress)
      throws IOException {

    return startLocator(port, logFile, false, bindAddress, (Properties) null, true, true, null);
  }


  /**
   * Starts a new distribution locator host by this VM that binds to the given network address.
   * <p>
   * The locator starts a AdminDistributedSystem configured with the given properties to provide the
   * system with a long-running process that can be relied on for stable membership information. The
   * locator will provide peer and cache server location services.
   *
   * @since GemFire 5.0
   *
   * @param port The port on which the locator will listen for membership information requests from
   *        new members
   * @param logFile The file to which the locator logs information. The directory that contains the
   *        log file is used as the output directory of the locator (see <code>-dir</code> option to
   *        the <code>gemfire</code> command).
   * @param bindAddress The IP address to which the locator's socket binds
   * @param dsProperties The properties used to configure the locator's DistributedSystem. If there
   *        are multiple locators, the "locators" property should be set. If multicast is being
   *        used, the "mcast-port" property should be set.
   *
   * @throws IllegalArgumentException If <code>port</code> is not in the range 0 to 65536
   * @throws org.apache.geode.SystemIsRunningException If another locator is already running in
   *         <code>outputDir</code>
   * @throws org.apache.geode.GemFireIOException If the directory containing the
   *         <code>logFile</code> does not exist or cannot be written to
   * @throws IOException If the locator cannot be started
   */
  public static Locator startLocatorAndDS(int port, File logFile, InetAddress bindAddress,
      java.util.Properties dsProperties) throws IOException {
    return startLocator(port, logFile, bindAddress, dsProperties, true, true, null);
  }

  /**
   * Starts a new distribution locator host by this VM that binds to the given network address.
   * <p>
   * The locator starts a AdminDistributedSystem configured with the given properties to provide the
   * system with a long-running process that can be relied on for stable membership information. The
   * locator will provide provide peer and cache server location services.
   *
   * @deprecated use a different startup method peerLocator and serverLocator parameters are ignored
   * @since GemFire 5.7
   *
   * @param port The port on which the locator will listen for membership information requests from
   *        new members
   * @param logFile The file to which the locator logs information. The directory that contains the
   *        log file is used as the output directory of the locator (see <code>-dir</code> option to
   *        the <code>gemfire</code> command).
   * @param bindAddress The IP address to which the locator's socket binds
   * @param dsProperties The properties used to configure the locator's DistributedSystem. If there
   *        are multiple locators, the "locators" property should be set. If multicast is being
   *        used, the "mcast-port" property should be set.
   * @param peerLocator True if the locator should provide membership information to peers in the
   *        distributed system.
   * @param serverLocator True if the locator should provide information about cache servers to
   *        clients connecting to the distributed system.
   * @param hostnameForClients the name to give to clients for connecting to this locator
   *
   * @throws IllegalArgumentException If <code>port</code> is not in the range 0 to 65536 or
   *         <code>peerLocator</code> and <code> serverLocator</code> are both false.
   * @throws org.apache.geode.SystemIsRunningException If another locator is already running in
   *         <code>outputDir</code>
   * @throws org.apache.geode.GemFireIOException If the directory containing the
   *         <code>logFile</code> does not exist or cannot be written to
   * @throws IOException If the locator cannot be started
   *
   * @since GemFire 5.7
   */
  public static Locator startLocatorAndDS(int port, File logFile, InetAddress bindAddress,
      java.util.Properties dsProperties, boolean peerLocator, boolean serverLocator,
      String hostnameForClients) throws IOException {
    return startLocator(port, logFile, bindAddress, dsProperties, true, true, hostnameForClients);
  }

  /**
   * all Locator methods that start locators should use this method to start the locator and its
   * distributed system
   */
  private static Locator startLocator(int port, File logFile, InetAddress bindAddress,
      java.util.Properties dsProperties, boolean peerLocator, boolean serverLocator,
      String hostnameForClients) throws IOException {
    return InternalLocator.startLocator(port, logFile, null, null, bindAddress, true, dsProperties,
        hostnameForClients);
  }

  /**
   * @deprecated as of 7.0 use startLocator(int, File, InetAddress, java.util.Properties,
   *             peerLocator, serverLocator, hostnameForClients) instead.
   */
  private static Locator startLocator(int port, File logFile, boolean startDistributedSystem,
      InetAddress bindAddress, java.util.Properties dsProperties, boolean peerLocator,
      boolean serverLocator, String hostnameForClients) throws IOException {
    return InternalLocator.startLocator(port, logFile, null, null, bindAddress,
        startDistributedSystem, dsProperties, hostnameForClients);
  }

  /**
   * Returns an unmodifiable <code>List</code> of all of the <code>Locator</code>s that are hosted
   * by this VM.
   *
   * @deprecated as of 7.0 use {@link #getLocator} instead
   */
  public static List<Locator> getLocators() {
    Locator result = getLocator();
    if (result == null) {
      return Collections.emptyList();
    } else {
      return Collections.singletonList(result);
    }
  }

  /**
   * Returns the locator if it exists in this JVM. Otherwise returns null.
   *
   * @return the locator that exists in this JVM; null if no locator.
   * @since GemFire 7.0
   */
  public static Locator getLocator() {
    return InternalLocator.getLocator();
  }

  /**
   * Examine the size of the collection of locators running in this VM
   *
   * @return the number of locators running in this VM
   * @deprecated as of 7.0 use {@link #hasLocator} instead.
   */
  public static boolean hasLocators() {
    return hasLocator();
  }

  /**
   * Returns true if a locator exists in this JVM.
   *
   * @return true if a locator exists in this JVM.
   * @since GemFire 7.0
   */
  public static boolean hasLocator() {
    return InternalLocator.hasLocator();
  }
  ///////////////////// Instance Methods /////////////////////

  /**
   * Returns the port on which this locator runs
   */
  public abstract Integer getPort();

  /**
   * Returns the distributed system started by this locator, if any
   */
  public abstract DistributedSystem getDistributedSystem();


  /**
   * Returns the log file to which this locator's output is written
   */
  public File getLogFile() {
    return this.logFile;
  }

  /**
   * Returns the IP address to which this locator's listening socket is bound.
   */
  public InetAddress getBindAddress() {
    return this.bindAddress;
  }

  /**
   * Returns the hostname that will be given to clients so that they can connect to this locator.
   * Returns <code>null</code> if clients should use the bind address.
   *
   * @since GemFire 5.7
   */
  public String getHostnameForClients() {
    String result = this.hostnameForClients;
    if (result != null && result.equals("")) {
      result = null;
    }
    return result;
  }

  /**
   * Indicates whether the locator provides peer location services to members
   *
   * @return if peer location is enabled
   */
  public abstract boolean isPeerLocator();

  /**
   * Indicates whether the locator provides server location services to clients
   *
   * @return if server location is enabled
   */
  public abstract boolean isServerLocator();


  /**
   * Stops this distribution locator.
   */
  public abstract void stop();

  /**
   * Returns a brief description of this <code>Locator</code>
   */
  @Override
  public String toString() {
    return String.format("Distribution Locator on %s",
        asString());
  }

  /**
   * Get the string representation of this <code>Locator</code> in host[port] format.
   */
  public String asString() {
    Object ba = this.bindAddress;
    if (ba == null) {
      try {
        ba = LocalHostUtil.getLocalHostName();
      } catch (java.net.UnknownHostException uh) {
      }
    }
    StringBuilder locatorString = new StringBuilder(String.valueOf(ba));
    Integer port = getPort();
    if (port != null && port.intValue() > 0) {
      locatorString.append('[').append(this.getPort()).append(']');
    }
    return locatorString.toString();
  }

  /**
   * Starts a distribution locator from the command line.
   * <p>
   * This method of starting the locator is provided as an alternative to the <i>gemfire
   * start-locator</i> command to give you complete control over the java virtual machine's
   * configuration.
   * <p>
   * The <i>gemfire stop-locator</i> command can be used to stop a locator that is started with this
   * class.
   * <p>
   * java org.apache.geode.distributed.Locator port [bind-address] [gemfire-properties-file] [peer]
   * [server]
   * <p>
   * port - the tcp/ip port that the locator should listen on. This is the port number that
   * applications will refer to in their <i>locators</i> property in gemfire.properties
   * <p>
   * bind-address - the tcp/ip address that the locator should bind to. This can be missing or be an
   * empty string, which causes the locator to listen on all host addresses.
   * <p>
   * gemfire-properties-file - the location of a gemfire.properties file to be used in configuring
   * the locator's distributed system. This can be missing or be an empty string, which will cause
   * the locator to use the default search for gemfire.properties.
   * <p>
   * peer - true to start the peer locator service, false to disable it. If unspecified, default to
   * true.
   * <p>
   * server - true to start the cache server locator service, false to disable it. If unspecified,
   * defaults to true.
   * <p>
   * hostname-for-clients - the ip address or host name that clients will be told to use to connect
   * to this locator. If unspecified, defaults to the bind-address.
   *
   * @deprecated as of Geode 1.4 use {@link org.apache.geode.distributed.LocatorLauncher
   *             "LocatorLauncher" to start a locator}
   */
  public static void main(String args[]) {
    org.apache.geode.internal.DistributionLocator.main(args);
  }

}
