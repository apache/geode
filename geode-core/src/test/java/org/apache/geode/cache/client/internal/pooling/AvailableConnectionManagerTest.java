/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.cache.client.internal.pooling;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import org.apache.geode.cache.client.internal.Connection;

public class AvailableConnectionManagerTest {

  private final AvailableConnectionManager instance = new AvailableConnectionManager();

  @Test
  public void useFirstReturnsNullGivenEmptyManager() {
    instance.getDeque().clear();

    Connection result = instance.useFirst();

    assertThat(result).isNull();
  }

  @Test
  public void useFirstReturnsExpectedConnectionGivenManagerWithOneItem() {
    Connection expected = createConnection();
    instance.getDeque().addFirst(expected);

    Connection result = instance.useFirst();

    assertThat(result).isSameAs(expected);
    assertThat(instance.getDeque()).isEmpty();
    verify(expected).activate();
  }

  @Test
  public void useFirstReturnsNullGivenManagerWithOneItemThatCantBeActivated() {
    Connection expected = createConnection();
    when(expected.activate()).thenReturn(false);
    instance.getDeque().addFirst(expected);

    Connection result = instance.useFirst();

    assertThat(result).isNull();
    assertThat(instance.getDeque()).isEmpty();
    verify(expected).activate();
  }

  @Test
  public void useFirstWithPredicateReturnsNullGivenEmptyManager() {
    instance.getDeque().clear();

    Connection result = instance.useFirst(c -> true);

    assertThat(result).isNull();
  }

  @Test
  public void useFirstWithPredicateReturnsExpectedGivenManagerWithOneItem() {
    Connection expected = createConnection();
    instance.getDeque().addFirst(expected);

    Connection result = instance.useFirst(c -> c == expected);

    assertThat(result).isSameAs(expected);
    assertThat(instance.getDeque()).isEmpty();
    verify(expected).activate();
  }

  @Test
  public void useFirstWithPredicateReturnsNullGivenManagerWithOneItemThatDoesNotMatch() {
    Connection expected = createConnection();
    instance.getDeque().addFirst(expected);

    Connection result = instance.useFirst(c -> false);

    assertThat(result).isNull();
    assertThat(instance.getDeque()).hasSize(1);
    verify(expected, never()).activate();
  }

  @Test
  public void useFirstWithPredicateReturnsNullGivenManagerWithOneItemThatCantBeActivated() {
    Connection expected = createConnection();
    when(expected.activate()).thenReturn(false);
    instance.getDeque().addFirst(expected);

    Connection result = instance.useFirst(c -> c == expected);

    assertThat(result).isNull();
    assertThat(instance.getDeque()).isEmpty();
    verify(expected).activate();
  }

  @Test
  public void useFirstWithPredicateReturnsNullGivenManagerWithOneItemThatDoesNotMatchAfterBeingActivated() {
    Connection expected = createConnection();
    when(expected.activate()).thenReturn(true);
    instance.getDeque().addFirst(expected);
    final AtomicBoolean firstTime = new AtomicBoolean(true);

    Connection result = instance.useFirst(c -> {
      if (firstTime.get()) {
        firstTime.set(false);
        return true;
      }
      return false;
    });

    assertThat(result).isNull();
    assertThat(instance.getDeque()).containsExactly(expected);
    verify(expected).activate();
    verify(expected).passivate(false);
  }

  @Test
  public void removeReturnsFalseGivenConnectionNotInManager() {
    instance.getDeque().clear();

    boolean result = instance.remove(createConnection());

    assertThat(result).isFalse();
  }

  @Test
  public void removeReturnsTrueGivenConnectionInManager() {
    Connection connection = createConnection();
    instance.getDeque().addFirst(connection);

    boolean result = instance.remove(connection);

    assertThat(result).isTrue();
  }

  @Test
  public void removeEmptiesDequeGivenConnectionInManager() {
    Connection connection = createConnection();
    instance.getDeque().addFirst(connection);

    instance.remove(connection);

    assertThat(instance.getDeque()).isEmpty();
  }

  @Test
  public void addFirstWithTrueAddsActiveConnectionToManager() {
    Connection connection = createConnection();

    instance.addFirst(connection, true);

    assertThat(instance.getDeque()).hasSize(1);
    verify(connection).isActive();
    verify(connection).passivate(true);
  }

  @Test
  public void addFirstWithFalseAddsActiveConnectionToManager() {
    Connection connection = createConnection();

    instance.addFirst(connection, false);

    assertThat(instance.getDeque()).hasSize(1);
    verify(connection).isActive();
    verify(connection).passivate(false);
  }

  @Test
  public void addFirstAddsInactiveConnectionToManager() {
    Connection connection = createConnection();
    when(connection.isActive()).thenReturn(false);

    instance.addFirst(connection, true);

    assertThat(instance.getDeque()).hasSize(1);
    verify(connection).isActive();
    verify(connection, never()).passivate(anyBoolean());
  }


  @Test
  public void addLastWithTrueAddsActiveConnectionToManager() {
    Connection connection = createConnection();

    instance.addLast(connection, true);

    assertThat(instance.getDeque()).hasSize(1);
    verify(connection).isActive();
    verify(connection).passivate(true);
  }

  @Test
  public void addLastWithFalseAddsActiveConnectionToManager() {
    Connection connection = createConnection();

    instance.addLast(connection, false);

    assertThat(instance.getDeque()).hasSize(1);
    verify(connection).isActive();
    verify(connection).passivate(false);
  }

  @Test
  public void addLastAddsInactiveConnectionToManager() {
    Connection connection = createConnection();
    when(connection.isActive()).thenReturn(false);

    instance.addLast(connection, true);

    assertThat(instance.getDeque()).hasSize(1);
    verify(connection).isActive();
    verify(connection, never()).passivate(anyBoolean());
  }

  @Test
  public void addFirstTakesPrecedenceOverAddLast() {
    Connection expected = createConnection();

    instance.addLast(createConnection(), true);
    instance.addFirst(expected, true);
    instance.addLast(createConnection(), true);
    Connection connection = instance.useFirst();

    assertThat(instance.getDeque()).hasSize(2);
    assertThat(connection).isSameAs(expected);
  }

  private Connection createConnection() {
    Connection result = mock(Connection.class);
    when(result.activate()).thenReturn(true);
    when(result.isActive()).thenReturn(true);
    return result;
  }
}
