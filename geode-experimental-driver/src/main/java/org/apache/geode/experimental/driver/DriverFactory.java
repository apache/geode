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
   * Creates a driver configured to use all the locators about which this driver factory knows.
   *
   * @return New driver.
   * @throws Exception
   */
  public Driver create() throws Exception {
    return new ProtobufDriver(locators);
  }
}
