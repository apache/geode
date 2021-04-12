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

package org.apache.geode.gradle.testing.dockerized;

import static java.util.Collections.list;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.List;

import org.gradle.api.UncheckedIOException;
import org.gradle.internal.remote.Address;
import org.gradle.internal.remote.ConnectionAcceptor;
import org.gradle.internal.remote.internal.inet.MultiChoiceAddress;

/**
 * Wraps a {@link ConnectionAcceptor} to give it a {@link MultiChoiceAddress} that processes in
 * Docker containers can use to connect to Gradle's messaging server.
 */
class DockerConnectionAcceptor implements ConnectionAcceptor {
  private static final List<InetAddress> DOCKER_ACCEPTABLE_ADDRESSES;

  static {
    try {
      DOCKER_ACCEPTABLE_ADDRESSES = list(NetworkInterface.getNetworkInterfaces()).stream()
          .filter(DockerConnectionAcceptor::isAcceptable)
          .flatMap(i -> list(i.getInetAddresses()).stream())
          .filter(DockerConnectionAcceptor::isAcceptable)
          .collect(toList());
    } catch (SocketException e) {
      throw new UncheckedIOException("Unable to identify usable addresses", e);
    }
  }

  private final MultiChoiceAddress address;
  private final ConnectionAcceptor delegate;

  /**
   * Creates a {@code MultiChoiceAddress} whose candidate addresses are acceptable for processes in
   * Docker containers to use to attempt to connect to Gradle's messaging server. The messaging
   * server will accept connections from one of those candidate addresses.
   */
  DockerConnectionAcceptor(ConnectionAcceptor delegate) {
    this.delegate = delegate;
    // The delegate's candidates are the host's loopback addresses, which processes in Docker
    // containers cannot use.
    MultiChoiceAddress original = (MultiChoiceAddress) delegate.getAddress();
    // Replace the delegate's unacceptable candidate addresses with acceptable ones.
    address = new MultiChoiceAddress(original.getCanonicalAddress(), original.getPort(),
        DOCKER_ACCEPTABLE_ADDRESSES);
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

  /**
   * Reports whether the candidate interface is acceptable for processes in Docker containers to use
   * to connect to Gradle's messaging server. An interface is acceptable if it satisfies all of:
   * <ul>
   *   <li>it is up</li>
   *   <li>it is not a loopback interface</li>
   *   <li>it is not a point to point interface (e.g. VPN)</li>
   */
  private static boolean isAcceptable(NetworkInterface candidate) {
    try {
      return !candidate.isLoopback()
          && !candidate.isPointToPoint()
          && candidate.isUp();
    } catch (SocketException ignored) {
      return false;
    }
  }

  /**
   * Reports whether the candidate address is acceptable for processes in Docker containers to use
   * to connect to Gradle's messaging server. An address is acceptable if it is reachable.
   */
  private static boolean isAcceptable(InetAddress candidate) {
    try {
      return candidate.isReachable(2000);
    } catch (IOException ignored) {
      return false;
    }
  }
}
