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

import static java.lang.String.format;
import static java.util.Objects.hash;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;
import static org.apache.commons.lang3.ObjectUtils.getIfNull;
import static org.apache.commons.lang3.StringUtils.isEmpty;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.annotations.internal.MakeNotSerializable;
import org.apache.geode.annotations.internal.SerializableCompatibility;
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
 * Serializable for RemoteLocatorJoinRequest and ServerLocationRequest.
 */
@MakeNotSerializable
public class DistributionLocatorId implements java.io.Serializable {
  private static final long serialVersionUID = 6587390186971937865L;

  private final InetAddress host;
  private final int port;
  private final @NotNull String bindAddress;
  /**
   * Serialization: Since this is {@code transient} it must be initialized after deserialization,
   * thus it can't be {@code final}.
   */
  private transient @NotNull SSLConfig sslConfig;
  @Deprecated
  @SerializableCompatibility("Required for upgrades from versions prior to Geode 1.2.0")
  @SuppressWarnings("unused")
  private final boolean peerLocator = true;
  @Deprecated
  @SerializableCompatibility("Required for upgrades from versions prior to Geode 1.2.0")
  @SuppressWarnings("unused")
  private final boolean serverLocator = true;
  private final String hostnameForClients;
  private String hostname;
  /**
   * Serialization: Not include in some older versions and may be {@code null} abd must be
   * initialized after deserialization, thus it can't be {@code final}.
   */
  private @NotNull String memberName;
  private final long timestamp;

  /**
   * Constructs a DistributionLocatorId with the given host and port.
   * This constructor is used for design base behavior.
   */
  public DistributionLocatorId(final @NotNull InetAddress host, final int port,
      final @Nullable String bindAddress, final @Nullable SSLConfig sslConfig) {
    this(host, null, port, bindAddress, sslConfig, null, null);
  }

  /**
   * Constructs a DistributionLocatorId with the given port and member name.
   * The host will be set to the local host.
   */
  public DistributionLocatorId(final int port, final @Nullable String bindAddress,
      final @Nullable String hostnameForClients, final @Nullable String memberName) {
    this(getLocalHostOrThrow(), null, port, bindAddress, null, hostnameForClients, memberName);
  }

  public DistributionLocatorId(final @NotNull InetAddress address,
      final @NotNull InternalLocator locator) {
    this(address, null, locator.getPort(),
        (locator.getBindAddressString() != null
            ? locator.getBindAddressString()
            : (locator.getBindAddress() != null ? locator.getBindAddress().getHostAddress()
                : null)),
        null,
        locator.getHostnameForClients(), null);
  }

  private DistributionLocatorId(final @Nullable InetAddress host, @Nullable String hostname,
      final int port,
      final @Nullable String bindAddress, final @Nullable SSLConfig sslConfig,
      final @Nullable String hostnameForClients, final @Nullable String memberName) {
    this.host = host;
    this.hostname = hostname;
    this.port = port;
    this.bindAddress = bindAddressOrDefault(bindAddress);
    this.sslConfig = sslConfigOrDefault(sslConfig);
    this.hostnameForClients = hostnameForClients;
    this.memberName = memberNameOrDefault(memberName);
    timestamp = System.currentTimeMillis();
  }

  /**
   * Unmarshall using default memberName.
   *
   * @see DistributionLocatorId#unmarshal(String, String)
   */
  public static @NotNull DistributionLocatorId unmarshal(final @NotNull String marshalled) {
    return unmarshal(marshalled, null);
  }

  /**
   * Constructs a DistributionLocatorId with a String of the form: hostname[port] or
   * hostname:bindaddress[port] or hostname@bindaddress[port]
   * and memberName
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
   * memberName example: locator1 or locator-ny1.
   * <p>
   */
  public static @NotNull DistributionLocatorId unmarshal(final @NotNull String marshalled,
      final @Nullable String memberName) {

    final int portStartIndex = marshalled.indexOf('[');
    final int portEndIndex = marshalled.indexOf(']');

    if (portStartIndex < 0 || portEndIndex < portStartIndex) {
      throw new IllegalArgumentException(
          format("%s is not in the form hostname[port].", marshalled));
    }

    int bindAddressIndex = marshalled.lastIndexOf('@');
    if (bindAddressIndex < 0) {
      bindAddressIndex = marshalled.lastIndexOf(':');
    }

    String hostname =
        marshalled.substring(0, bindAddressIndex > -1 ? bindAddressIndex : portStartIndex);

    if (hostname.indexOf(':') >= 0) {
      bindAddressIndex = marshalled.lastIndexOf('@');
      hostname = marshalled.substring(0, bindAddressIndex > -1 ? bindAddressIndex : portStartIndex);
    }

    InetAddress host = getInetAddressOrNull(hostname);

    final int port;
    try {
      port = Integer.parseInt(marshalled.substring(portStartIndex + 1, portEndIndex));
    } catch (NumberFormatException nfe) {
      throw new IllegalArgumentException(
          format("%s does not contain a valid port number", marshalled));
    }

    final String bindAddress =
        substringAfterFoundOrNull(marshalled, bindAddressIndex, portStartIndex);
    final String hostnameForClients = parseHostnameForClients(marshalled);

    return new DistributionLocatorId(host, hostname, port, bindAddress, null, hostnameForClients,
        memberName);
  }

  private static @Nullable String parseHostnameForClients(final @NotNull String marshalled) {
    final int optionsIndex = marshalled.indexOf(',');
    if (optionsIndex > 0) {
      final String[] options = marshalled.substring(optionsIndex).split(",");
      for (final String option : options) {
        final String[] optionFields = option.split("=");
        if (optionFields.length == 2) {
          final String fieldName = optionFields[0].toLowerCase();
          switch (fieldName) {
            case "hostname-for-clients":
              return optionFields[1];
            case "peer":
            case "server":
              // these settings are deprecated
              break;
            default:
              throw new IllegalArgumentException(marshalled + " invalid option " + fieldName
                  + ". valid options are \"peer\", \"server\" and \"hostname-for-clients\"");
          }
        }
      }
    }
    return null;
  }

  /**
   * Returns marshaled string that is compatible as input for unmarshal.
   *
   * @see #unmarshal(String)
   * @see #unmarshal(String, String)
   */
  public @NotNull String marshal() {
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

  /**
   * Returns marshaled string that is compatible as input for unmarshal preferring the
   * hostnameForClients or bindAddress values before the host value.
   *
   * @see #unmarshal(String)
   * @see #unmarshal(String, String)
   */
  public @NotNull String marshalForClients() {
    StringBuilder sb = new StringBuilder();

    if (!isEmpty(hostnameForClients)) {
      sb.append(hostnameForClients);
    } else if (!isEmpty(bindAddress)) {
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

  public @NotNull SSLConfig getSSLConfig() {
    return sslConfig;
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
  public @NotNull HostAndPort getHost() throws UnknownHostException {
    if (host == null && hostname == null) {
      throw new UnknownHostException("locator ID has no hostname or resolved inet address");
    }
    return new HostAndPort(null == host ? hostname : host.getHostName(), port);
  }

  /** returns the host name */
  public @NotNull String getHostName() {
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
  public @NotNull String getBindAddress() {
    return bindAddress;
  }

  /**
   * @since GemFire 5.7
   */
  public @NotNull String getHostnameForClients() {
    return hostnameForClients;
  }

  public @NotNull String getMemberName() {
    return memberName;
  }

  public long getTimeStamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return "DistributionLocatorId{" +
        "host=" + host +
        ", port=" + port +
        ", bindAddress='" + bindAddress + '\'' +
        ", sslConfig=" + sslConfig +
        ", hostnameForClients='" + hostnameForClients + '\'' +
        ", hostname='" + hostname + '\'' +
        ", memberName='" + memberName + '\'' +
        ", timestamp=" + timestamp +
        '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DistributionLocatorId)) {
      return false;
    }
    final DistributionLocatorId that = (DistributionLocatorId) o;
    return port == that.port && Objects.equals(host, that.host) && bindAddress.equals(
        that.bindAddress);
  }

  @Override
  public int hashCode() {
    return hash(host, port, bindAddress);
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
   * Required to initialize non-null values for transient or missing fields.
   */
  private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
    in.defaultReadObject();

    sslConfig = sslConfigOrDefault(null);
    memberName = memberNameOrDefault(memberName);
  }

  private static @NotNull String bindAddressOrDefault(final @Nullable String bindAddress) {
    return defaultIfNull(bindAddress, DistributionConfig.DEFAULT_BIND_ADDRESS);
  }

  private static @NotNull SSLConfig sslConfigOrDefault(final @Nullable SSLConfig sslConfig) {
    return getIfNull(sslConfig, () -> SSLConfig.builder().build());
  }

  private static @NotNull String memberNameOrDefault(final @Nullable String memberName) {
    return defaultIfNull(memberName, DistributionConfig.DEFAULT_NAME);
  }

  private static @NotNull InetAddress getLocalHostOrThrow() {
    try {
      return LocalHostUtil.getLocalHost();
    } catch (UnknownHostException e) {
      throw new InternalGemFireException("Failed getting local host", e);
    }
  }

  private static @Nullable InetAddress getInetAddressOrNull(@NotNull String hostname) {
    try {
      return InetAddress.getByName(hostname);
    } catch (UnknownHostException ignore) {
    }
    return null;
  }

  private static @Nullable String substringAfterFoundOrNull(final @NotNull String value,
      final int found, final int end) {
    return found < 0 ? null : value.substring(found + 1, end);
  }

}
