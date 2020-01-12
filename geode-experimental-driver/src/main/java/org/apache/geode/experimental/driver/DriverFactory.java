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
package org.apache.geode.experimental.driver;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.annotations.Experimental;

/**
 * This is an experimental driver for connecting a client to a geode cluster. This driver is still
 * under development. For a working, full featured client, use ClientCache in geode-core. This
 * driver factory supports the builder style of chaining together mutators.
 *
 * <strong>This code is an experimental prototype and is presented "as is" with no warranty,
 * suitability, or fitness of purpose implied.</strong>
 */
@Experimental
public class DriverFactory {
  /**
   * Set of Internet-address-or-host-name/port pairs of the locators to use to find GemFire servers
   * that have Protobuf enabled.
   */
  private Set<InetSocketAddress> locators = new HashSet<InetSocketAddress>();

  /**
   * User identity as a string.
   */
  private String username = null;

  /**
   * User proof as a string.
   */
  private String password = null;
  private ValueSerializer serializer = new NoOpSerializer();

  /**
   * Path to SSL key store; SSL is <em>not</em> used if <code>null</code>.
   */
  private String keyStorePath;

  /**
   * Path to SSL trust store; SSL is <em>not</em> used if <code>null</code>.
   */
  private String trustStorePath;

  /**
   * Space-separated list of the SSL protocols to enable.
   */
  private String protocols;

  /**
   * Space-separated list of the SSL cipher suites to enable.
   */
  private String ciphers;

  /**
   * Adds a locator at <code>host</code> and <code>port</code> to the set of locators to use.
   *
   * @param host Internet address or host name.
   * @param port Port number.
   * @return This driver factory.
   */
  public DriverFactory addLocator(String host, int port) {
    this.locators.add(new InetSocketAddress(host, port));
    return this;
  }

  /**
   * Specifies the user name with which to authenticate with the server.
   *
   * @param username User identity as a string; may be <code>null</code>.
   * @return This driver factory.
   */
  public DriverFactory setUsername(String username) {
    this.username = username;
    return this;
  }

  /**
   * Specifies the password with which to authenticate with the server.
   *
   * @param password User proof as a string; may be <code>null</code>.
   * @return This driver factory.
   */
  public DriverFactory setPassword(String password) {
    this.password = password;
    return this;
  }

  /**
   * Specifies the key store to use with SSL.
   *
   * @param keyStorePath Path to the SSL key store.
   * @return This driver factory.
   */
  public DriverFactory setKeyStorePath(String keyStorePath) {
    this.keyStorePath = keyStorePath;
    return this;
  }

  /**
   * Specifies the trust store to use with SSL.
   *
   * @param trustStorePath Path to the SSL trust store.
   * @return This driver factory.
   */
  public DriverFactory setTrustStorePath(String trustStorePath) {
    this.trustStorePath = trustStorePath;
    return this;
  }

  /**
   * Specifies the protocols to enable.
   *
   * @param protocols Space-separated list of the SSL protocols to enable.
   * @return This driver factory.
   */
  public DriverFactory setProtocols(String protocols) {
    this.protocols = protocols;
    return this;
  }

  /**
   * Specifies the cipher suites to enable.
   *
   * @param ciphers Space-separated list of the SSL cipher suites to enable.
   * @return This driver factory.
   */
  public DriverFactory setCiphers(String ciphers) {
    this.ciphers = ciphers;
    return this;
  }

  /**
   * Creates a driver configured to use all the locators about which this driver factory knows.
   *
   * @return New driver.
   */
  public Driver create() throws Exception {
    return new ProtobufDriver(locators, username, password, keyStorePath, trustStorePath, protocols,
        ciphers, serializer);
  }

  public DriverFactory setValueSerializer(ValueSerializer serializer) {
    this.serializer = serializer;
    return this;
  }
}
