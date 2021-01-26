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
 *
 */

package org.apache.geode.gradle.test.dockerized;

import static java.util.Collections.list;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.List;

import org.gradle.api.UncheckedIOException;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.internal.remote.Address;
import org.gradle.internal.remote.ConnectionAcceptor;
import org.gradle.internal.remote.internal.inet.MultiChoiceAddress;

/**
 * A ConnectionAcceptor that delegates to another, but replaces its MultiChoiceAddress with one
 * that uses addresses from only non-loopback interfaces.
 * TODO: Use a custom InetAddressFactory to supply Docker-friendly local/remote addresses.
 * Then we can get rid of this class and probably DockerMessagingServer.
 */
class NonLoopbackConnectionAcceptor implements ConnectionAcceptor {
  private static final Logger LOGGER = Logging.getLogger(DockerMessagingServer.class);
  private static final List<InetAddress> ADDRESSES;

  static {
    try {
      ADDRESSES = list(NetworkInterface.getNetworkInterfaces()).stream()
          .filter(NonLoopbackConnectionAcceptor::isUsable)
          .flatMap(i -> list(i.getInetAddresses()).stream())
//          .filter(NonLoopbackConnectionAcceptor::isUsable)
          .collect(toList());
    } catch (SocketException e) {
      throw new UncheckedIOException("Unable to identify usable addresses", e);
    }
  }

  private final MultiChoiceAddress address;
  private final ConnectionAcceptor delegate;

  NonLoopbackConnectionAcceptor(ConnectionAcceptor delegate) {
    this.delegate = delegate;
    MultiChoiceAddress original = (MultiChoiceAddress) delegate.getAddress();
    address = new MultiChoiceAddress(original.getCanonicalAddress(), original.getPort(), ADDRESSES);
    ;
  }

  @Override
  public Address getAddress() {
    return address;
  }

  @Override
  public void requestStop() {
    delegate.requestStop();
  }

  @Override
  public void stop() {
    delegate.stop();
  }

  // An interface is usable if it is up and not a loopback interface.
  private static boolean isUsable(NetworkInterface networkInterface) {
    try {
      return networkInterface.isUp() && !networkInterface.isLoopback();
    } catch (SocketException ignored) {
      LOGGER.warn("Unable to inspect interface " + networkInterface);
      return false;
    }
  }

  // An address is usable if it is reachable and not a loopback address.
  private static boolean isUsable(InetAddress address) {
    try {
      return !address.isLoopbackAddress() && address.isReachable(1000);
    } catch (IOException ignored) {
      LOGGER.warn("Unable to inspect interface " + address);
      return false;
    }
  }
}
