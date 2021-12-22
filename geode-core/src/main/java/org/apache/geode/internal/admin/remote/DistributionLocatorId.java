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

package org.apache.geode.internal.admin.remote;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.tcpserver.HostAndPort;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.net.SSLConfig;
import org.apache.geode.internal.net.SocketCreator;

/**
 * Identifies the host, port, and bindAddress a distribution locator is listening on.
 * Also identifies member name of the distribution locator. This is used to improve
 * locator discovery logic.
 * If member name is set to null, then design base logic will be used.
 *
 */
public class DistributionLocatorId implements java.io.Serializable {
  private static final long serialVersionUID = 6587390186971937865L;

  private InetAddress host;
  private final int port;
  private final String bindAddress;
  private transient SSLConfig sslConfig;
  // the following two fields are not used but are retained for backward compatibility
  // as this class is Serializable and is used in WAN locator information exchange
  private final boolean peerLocator = true;
  private final boolean serverLocator = true;
  private String hostnameForClients;
  private String hostname;
  // added due to improvement for cloud native environment
  private String membername;
  private final long timestamp;


  /**
   * Constructs a DistributionLocatorId with the given host and port.
   * This constructor is used for design base behavior.
   *
   */
  public DistributionLocatorId(InetAddress host, int port, String bindAddress,
      SSLConfig sslConfig) {
    this.host = host;
    this.port = port;
    this.bindAddress = validateBindAddress(bindAddress);
    this.sslConfig = validateSSLConfig(sslConfig);
    membername = DistributionConfig.DEFAULT_NAME;
    timestamp = 0;

  }

  public DistributionLocatorId(int port, String bindAddress) {
    this(port, bindAddress, null);
  }

  public DistributionLocatorId(int port, String bindAddress, String hostnameForClients) {
    this(port, bindAddress, hostnameForClients, DistributionConfig.DEFAULT_NAME);
  }

  /**
   * Constructs a DistributionLocatorId with the given port and member name.
   * The host will be set to the local host.
   *
   */
  public DistributionLocatorId(int port, String bindAddress, String hostnameForClients,
      String membername) {
    try {
      host = LocalHostUtil.getLocalHost();
    } catch (UnknownHostException ex) {
      throw new InternalGemFireException(
          "Failed getting local host", ex);
    }
    this.port = port;
    this.bindAddress = validateBindAddress(bindAddress);
    sslConfig = validateSSLConfig(null);
    this.hostnameForClients = hostnameForClients;
    if (membername == null) {
      this.membername = DistributionConfig.DEFAULT_NAME;
    } else {
      this.membername = membername;
    }
    timestamp = System.currentTimeMillis();
  }

  public DistributionLocatorId(InetAddress host, int port, String bindAddress, SSLConfig sslConfig,
      String hostnameForClients) {
    this.host = host;
    this.port = port;
    this.bindAddress = validateBindAddress(bindAddress);
    this.sslConfig = validateSSLConfig(sslConfig);
    this.hostnameForClients = hostnameForClients;
    membername = DistributionConfig.DEFAULT_NAME;
    timestamp = 0;

  }


  /**
   * Constructs a DistributionLocatorId with a String of the form: hostname[port] or
   * hostname:bindaddress[port] or hostname@bindaddress[port]
   * <p>
   * The :bindaddress portion is optional. hostname[port] is the more common form.
   * <p>
   * Example: merry.gemstone.com[7056]<br>
   * Example w/ bind address: merry.gemstone.com:81.240.0.1[7056], or
   * merry.gemstone.com@fdf0:76cf:a0ed:9449::16[7056]
   * <p>
   * Use bindaddress[port] or hostname[port]. This object doesn't need to differentiate between the
   * two.
   */
  public DistributionLocatorId(String marshalled) {
    this(marshalled, DistributionConfig.DEFAULT_NAME);
  }

  /**
   * Constructs a DistributionLocatorId with a String of the form: hostname[port] or
   * hostname:bindaddress[port] or hostname@bindaddress[port]
   * and membername
   * <p>
   * The :bindaddress portion is optional. hostname[port] is the more common form.
   * <p>
   * Example: merry.gemstone.com[7056]<br>
   * Example w/ bind address: merry.gemstone.com:81.240.0.1[7056], or
   * merry.gemstone.com@fdf0:76cf:a0ed:9449::16[7056]
   * <p>
   * Use bindaddress[port] or hostname[port]. This object doesn't need to differentiate between the
   * two.
   * <p>
   * Membername example: locator1 or locator-ny1.
   * <p>
   */
  public DistributionLocatorId(String marshalled, String membername) {
    if (membername == null) {
      this.membername = DistributionConfig.DEFAULT_NAME;
    } else {
      this.membername = membername;
    }
    timestamp = System.currentTimeMillis();

    final int portStartIdx = marshalled.indexOf('[');
    final int portEndIdx = marshalled.indexOf(']');

    if (portStartIdx < 0 || portEndIdx < portStartIdx) {
      throw new IllegalArgumentException(
          String.format("%s is not in the form hostname[port].",
              marshalled));
    }

    int bindIdx = marshalled.lastIndexOf('@');
    if (bindIdx < 0) {
      bindIdx = marshalled.lastIndexOf(':');
    }

    hostname = marshalled.substring(0, bindIdx > -1 ? bindIdx : portStartIdx);

    if (hostname.indexOf(':') >= 0) {
      bindIdx = marshalled.lastIndexOf('@');
      hostname = marshalled.substring(0, bindIdx > -1 ? bindIdx : portStartIdx);
    }


    try {
      host = InetAddress.getByName(hostname);
    } catch (UnknownHostException ex) {
      host = null;
    }

    try {
      port = Integer.parseInt(marshalled.substring(portStartIdx + 1, portEndIdx));
    } catch (NumberFormatException nfe) {
      throw new IllegalArgumentException(
          String.format("%s does not contain a valid port number",
              marshalled));
    }

    if (bindIdx > -1) {
      // found a bindaddress
      bindAddress = validateBindAddress(marshalled.substring(bindIdx + 1, portStartIdx));
    } else {
      bindAddress = validateBindAddress(DistributionConfig.DEFAULT_BIND_ADDRESS);
    }
    sslConfig = validateSSLConfig(null);

    int optionsIndex = marshalled.indexOf(',');
    if (optionsIndex > 0) {
      String[] options = marshalled.substring(optionsIndex).split(",");
      for (int i = 0; i < options.length; i++) {
        String[] optionFields = options[i].split("=");
        if (optionFields.length == 2) {
          if (optionFields[0].equalsIgnoreCase("peer")) {
            // this setting is deprecated
            // this.peerLocator = Boolean.valueOf(optionFields[1]).booleanValue();
          } else if (optionFields[0].equalsIgnoreCase("server")) {
            // this setting is deprecated
            // this.serverLocator = Boolean.valueOf(optionFields[1]).booleanValue();
          } else if (optionFields[0].equalsIgnoreCase("hostname-for-clients")) {
            hostnameForClients = optionFields[1];
          } else {
            throw new IllegalArgumentException(marshalled + " invalid option " + optionFields[0]
                + ". valid options are \"peer\", \"server\" and \"hostname-for-clients\"");
          }
        }
      }
    }
  }

  public DistributionLocatorId(InetAddress address, Locator locator) {
    this(address, locator.getPort(),
        (((InternalLocator) locator).getBindAddressString() != null
            ? ((InternalLocator) locator).getBindAddressString()
            : (locator.getBindAddress() != null ? locator.getBindAddress().getHostAddress()
                : null)),
        null,
        locator.getHostnameForClients());
  }

  /**
   * Returns marshaled string that is compatible as input for
   * {@link #DistributionLocatorId(String)}.
   */
  public String marshal() {
    StringBuilder sb = new StringBuilder();
    sb.append(host.getHostAddress());
    if (!bindAddress.isEmpty()) {
      if (bindAddress.contains(":")) {
        sb.append('@');
      } else {
        sb.append(':');
      }
      sb.append(bindAddress);
    }
    sb.append('[').append(port).append(']');
    return sb.toString();
  }

  private SSLConfig validateSSLConfig(SSLConfig sslConfig) {
    if (sslConfig == null) {
      return new SSLConfig.Builder().build(); // uses defaults
    }
    return sslConfig;
  }

  public SSLConfig getSSLConfig() {
    return sslConfig;
  }

  public void setSSLConfig(SSLConfig sslConfig) {
    this.sslConfig = validateSSLConfig(sslConfig);
  }

  /** Returns the communication port. */
  public int getPort() {
    return port;
  }

  /**
   * Returns the resolved InetSocketAddress of the locator We cache the InetAddress if hostname is
   * ipString Otherwise we create InetAddress each time.
   *
   **/
  public HostAndPort getHost() throws UnknownHostException {
    if (host == null && hostname == null) {
      throw new UnknownHostException("locator ID has no hostname or resolved inet address");
    }
    String addr = hostname;
    if (host != null) {
      addr = host.getHostName();
    }
    return new HostAndPort(addr, port);
  }

  /** returns the host name */
  public String getHostName() {
    if (hostname == null) {
      hostname = host.getHostName();
    }
    return hostname;
  }

  /** Returns true if this is a multicast address:port */
  public boolean isMcastId() {
    return host.isMulticastAddress();
  }

  /**
   * Returns the bindAddress; value is "" unless host has multiple network interfaces.
   */
  public String getBindAddress() {
    return bindAddress;
  }

  /**
   * @since GemFire 5.7
   */
  public String getHostnameForClients() {
    return hostnameForClients;
  }

  public String getMemberName() {
    if (membername == null) {
      membername = DistributionConfig.DEFAULT_NAME;
    }
    return membername;
  }

  public long getTimeStamp() {
    return timestamp;
  }

  // private String hostNameToString() {
  // if (this.host.isMulticastAddress()) {
  // return this.host.getHostAddress();
  // } else {
  // return this.host.getHostName();
  // }
  // }

  /** Returns the default bindAddress if bindAddress is null. */
  private String validateBindAddress(String bindAddress) {
    if (bindAddress == null) {
      return DistributionConfig.DEFAULT_BIND_ADDRESS;
    }
    return bindAddress;
  }

  /**
   * Returns a string representation of the object.
   *
   * @return a string representation of the object
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    // If hostnameForClients is set, use that
    if (hostnameForClients != null && hostnameForClients.length() > 0) {
      sb.append(hostnameForClients);
    } else if (bindAddress != null && bindAddress.length() > 0) {
      // if bindAddress then use that instead of host...
      sb.append(bindAddress);
    } else {
      if (isMcastId()) {
        sb.append(host.getHostAddress());
      } else {
        sb.append(SocketCreator.getHostName(host));
      }
    }

    sb.append("[").append(port).append("]");
    return sb.toString();
  }

  /**
   * Indicates whether some other object is "equal to" this one.
   *
   * @param other the reference object with which to compare.
   * @return true if this object is the same as the obj argument; false otherwise.
   *
   */
  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (other == null) {
      return false;
    }
    if (!(other instanceof DistributionLocatorId)) {
      return false;
    }
    final DistributionLocatorId that = (DistributionLocatorId) other;

    if (!Objects.equals(host, that.host)) {
      return false;
    }
    if (port != that.port) {
      return false;
    }
    return StringUtils.equals(bindAddress, that.bindAddress);
  }

  /**
   *
   * In case both objects have same member name, it will compare all other parameters
   *
   * @param other the reference object with which to compare.
   * @return true if this object is the same as the obj argument; false otherwise.
   */
  public boolean detailCompare(Object other) {
    if (other == this) {
      return true;
    }
    if (other == null) {
      return false;
    }
    if (!(other instanceof DistributionLocatorId)) {
      return false;
    }
    final DistributionLocatorId that = (DistributionLocatorId) other;

    if (!StringUtils.equals(hostnameForClients, that.hostnameForClients)) {
      return false;
    }
    if (!Objects.equals(host, that.host)) {
      return false;
    }
    if (port != that.port) {
      return false;
    }
    return StringUtils.equals(bindAddress, that.bindAddress);
  }

  /**
   * Returns a hash code for the object. This method is supported for the benefit of hashtables such
   * as those provided by java.util.Hashtable.
   *
   * @return the integer 0 if description is null; otherwise a unique integer.
   */
  @Override
  public int hashCode() {
    int result = 17;
    final int mult = 37;

    result = mult * result + (host == null ? 0 : host.hashCode());
    result = mult * result + port;
    result = mult * result + (bindAddress == null ? 0 : bindAddress.hashCode());

    return result;
  }

  /**
   * Converts a collection of {@link Locator} instances to a collection of DistributionLocatorId
   * instances. Note this will use {@link LocalHostUtil#getLocalHost()} as the host for
   * DistributionLocatorId. This is because all instances of Locator are local only.
   *
   * @param locators collection of Locator instances
   * @return collection of DistributionLocatorId instances
   * @see Locator
   */
  public static Collection<DistributionLocatorId> asDistributionLocatorIds(
      Collection<Locator> locators) throws UnknownHostException {
    if (locators.isEmpty()) {
      return Collections.emptyList();
    }
    Collection<DistributionLocatorId> locatorIds = new ArrayList<>();
    for (Locator locator : locators) {
      DistributionLocatorId locatorId =
          new DistributionLocatorId(LocalHostUtil.getLocalHost(), locator);
      locatorIds.add(locatorId);
    }
    return locatorIds;
  }

  /**
   * Marshals a collection of {@link Locator} instances to a collection of DistributionLocatorId
   * instances. Note this will use {@link LocalHostUtil#getLocalHost()} as the host for
   * DistributionLocatorId. This is because all instances of Locator are local only.
   *
   * @param locatorIds collection of DistributionLocatorId instances
   * @return collection of String instances
   * @see #marshal()
   */
  public static Collection<String> asStrings(Collection<DistributionLocatorId> locatorIds) {
    if (locatorIds.isEmpty()) {
      return Collections.emptyList();
    }
    Collection<String> strings = new ArrayList<>();
    for (DistributionLocatorId locatorId : locatorIds) {
      strings.add(locatorId.marshal());
    }
    return strings;
  }

}
