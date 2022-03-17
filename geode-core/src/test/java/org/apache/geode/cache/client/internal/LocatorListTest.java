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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.geode.cache.client.internal.LocatorList.SOCKET_ADDRESS_COMPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import org.apache.geode.distributed.internal.tcpserver.HostAndPort;

class LocatorListTest {

  @Test
  void comparatorReturns0WhenBothHostNamesAreNull() {
    assertThat(SOCKET_ADDRESS_COMPARATOR.compare(new HostAndPort(null, 1234),
        new HostAndPort(null, 1234))).isEqualTo(0);
  }

  /**
   * The explicit order of these isn't important, only that it be consistent.
   */
  @Test
  void comparatorReturnsNot0WhenEitherHostNameIsNull() {
    assertThat(SOCKET_ADDRESS_COMPARATOR.compare(new HostAndPort("locator1", 1234),
        new HostAndPort(null, 1234))).isGreaterThan(0);

    assertThat(SOCKET_ADDRESS_COMPARATOR.compare(new HostAndPort(null, 1234),
        new HostAndPort("locator2", 1234))).isLessThan(0);
  }

  @Test
  void comparatorReturns0WhenBothHostNamesAreTheSameAndPortIsSame() {
    assertThat(SOCKET_ADDRESS_COMPARATOR.compare(new HostAndPort("locator1", 1234),
        new HostAndPort("locator1", 1234))).isEqualTo(0);
  }

  /**
   * The explicit order of these isn't important, only that it be consistent.
   */
  @Test
  void comparatorReturnsNot0WhenEitherHostNameIsDifferent() {
    assertThat(SOCKET_ADDRESS_COMPARATOR.compare(new HostAndPort("locator1", 1234),
        new HostAndPort("locator2", 1234))).isLessThan(0);

    assertThat(SOCKET_ADDRESS_COMPARATOR.compare(new HostAndPort("locator2", 1234),
        new HostAndPort("locator1", 1234))).isGreaterThan(0);
  }

  @Test
  void comparatorReturnsNot0WhenBothHostNamesAreTheSameAndPortIsDifferent() {
    assertThat(SOCKET_ADDRESS_COMPARATOR.compare(new HostAndPort("locator1", 1235),
        new HostAndPort("locator1", 1234))).isGreaterThan(0);

    assertThat(SOCKET_ADDRESS_COMPARATOR.compare(new HostAndPort("locator1", 1234),
        new HostAndPort("locator1", 1235))).isLessThan(0);
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  void constructorThrowsNullPointerException() {
    assertThatThrownBy(() -> new LocatorList(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void getLocatorAddressesReturnsLocatorsInSortedOrder() {
    final List<HostAndPort> locators = asList(
        new HostAndPort("locator2", 1234),
        new HostAndPort("locator1", 1234));

    final LocatorList locatorList = new LocatorList(locators);

    assertThat(locatorList.getLocatorAddresses()).containsExactly(
        new HostAndPort("locator1", 1234),
        new HostAndPort("locator2", 1234));
  }

  @Test
  void getLocatorsReturnsInetSocketAddressesInOrderInNewList() {
    final List<HostAndPort> locators = asList(
        new HostAndPort("locator2", 1234),
        new HostAndPort("locator1", 1234));

    final LocatorList locatorList = new LocatorList(locators);

    final List<InetSocketAddress> locators1 = locatorList.getLocators();
    final List<InetSocketAddress> locators2 = locatorList.getLocators();

    assertThat(locators1)
        .isNotNull()
        .isEqualTo(locators2)
        .isNotSameAs(locators2)
        .containsExactly(InetSocketAddress.createUnresolved("locator1", 1234),
            InetSocketAddress.createUnresolved("locator2", 1234));
  }

  @Test
  void getSizeReturnsCorrectSizes() {
    assertThat(new LocatorList(emptyList()).size()).isEqualTo(0);

    assertThat(new LocatorList(singletonList(
        new HostAndPort("locator2", 1234))).size()).isEqualTo(1);

    assertThat(new LocatorList(asList(
        new HostAndPort("locator2", 1234),
        new HostAndPort("locator1", 1234))).size()).isEqualTo(2);
  }

  @Test
  void iteratorReturnsExhaustedIteratorWhenEmpty() {
    final Iterator<@NotNull HostAndPort> iterator = new LocatorList(emptyList()).iterator();
    assertThat(iterator).isExhausted();
    assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  void iteratorReturnsIteratorWithEntriesWhenNotEmpty() {
    final HostAndPort locator1 = new HostAndPort("locator1", 1234);
    final HostAndPort locator2 = new HostAndPort("locator2", 1234);
    final Iterator<@NotNull HostAndPort> iterator = new LocatorList(asList(locator1, locator2))
        .iterator();

    assertThat(iterator).hasNext();
    assertThat(iterator.next()).isSameAs(locator1);
    assertThat(iterator).hasNext();
    assertThat(iterator.next()).isSameAs(locator2);
    assertThat(iterator).isExhausted();
    assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  void iteratorReturnsIteratorThatDoesNotSupportRemoveBeforeOrAfterNext() {
    final HostAndPort locator1 = new HostAndPort("locator1", 1234);
    final HostAndPort locator2 = new HostAndPort("locator2", 1234);
    final Iterator<@NotNull HostAndPort> iterator = new LocatorList(asList(locator1, locator2))
        .iterator();

    assertThatThrownBy(iterator::remove).isInstanceOf(UnsupportedOperationException.class);
    iterator.next();
    assertThatThrownBy(iterator::remove).isInstanceOf(UnsupportedOperationException.class);
  }

}
