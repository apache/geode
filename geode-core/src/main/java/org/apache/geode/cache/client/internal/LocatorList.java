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

package org.apache.geode.cache.client.internal;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.internal.tcpserver.HostAndPort;

/**
 * A list of locators, which remembers the last known good locator.
 */
class LocatorList implements Iterable<@NotNull HostAndPort> {
  /**
   * A Comparator used to sort a list of locator addresses. This should not be used in other ways as
   * it can return zero when two addresses aren't actually equal.
   */
  @Immutable
  static final Comparator<@NotNull HostAndPort> SOCKET_ADDRESS_COMPARATOR =
      (address, otherAddress) -> {
        final InetSocketAddress inetSocketAddress = address.getSocketInetAddress();
        final InetSocketAddress otherInetSocketAddress = otherAddress.getSocketInetAddress();

        final int result = StringUtils.compare(inetSocketAddress.getHostString(),
            otherInetSocketAddress.getHostString());
        if (result != 0) {
          return result;
        } else {
          return Integer.compare(inetSocketAddress.getPort(), otherInetSocketAddress.getPort());
        }
      };

  private final List<@NotNull HostAndPort> locators;
  private final AtomicInteger currentLocatorIndex = new AtomicInteger();

  public LocatorList(@NotNull List<@NotNull HostAndPort> locators) {
    locators.sort(SOCKET_ADDRESS_COMPARATOR);
    this.locators = Collections.unmodifiableList(locators);
  }

  public @NotNull List<@NotNull InetSocketAddress> getLocators() {
    final List<@NotNull InetSocketAddress> addresses = new ArrayList<>(locators.size());
    for (final HostAndPort locator : locators) {
      addresses.add(locator.getSocketInetAddress());
    }
    return addresses;
  }

  public @NotNull List<@NotNull HostAndPort> getLocatorAddresses() {
    return locators;
  }

  public int size() {
    return locators.size();
  }

  @Override
  public @NotNull Iterator<@NotNull HostAndPort> iterator() {
    return new LocatorIterator();
  }

  @Override
  public String toString() {
    return locators.toString();
  }

  /**
   * An iterator which iterates over all the controllers, starting at the last known good
   * controller.
   */
  private class LocatorIterator implements Iterator<@NotNull HostAndPort> {
    private final int lastKnownGoodIndex = currentLocatorIndex.get();
    private int currentIndex = 0;

    @Override
    public boolean hasNext() {
      return currentIndex < locators.size();
    }

    @Override
    public @NotNull HostAndPort next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      } else {
        final int index = (currentIndex + lastKnownGoodIndex) % locators.size();
        final HostAndPort nextLocator = locators.get(index);
        currentLocatorIndex.set(index);
        currentIndex++;
        return nextLocator;
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
