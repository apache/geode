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
package org.apache.geode.internal.cache;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.client.internal.ServerRegionProxy;

public class LocalRegionTest {
  private LocalRegion region;
  private EntryEventImpl event;
  private ServerRegionProxy serverRegionProxy;
  private Operation operation;
  private CancelCriterion cancelCriterion;
  private RegionAttributes regionAttributes;

  private final Object key = new Object();
  private final String value = "value";

  @Before
  public void setup() {
    region = mock(LocalRegion.class);
    event = mock(EntryEventImpl.class);
    serverRegionProxy = mock(ServerRegionProxy.class);
    cancelCriterion = mock(CancelCriterion.class);
    regionAttributes = mock(RegionAttributes.class);

    when(region.getServerProxy()).thenReturn(serverRegionProxy);
    when(event.isFromServer()).thenReturn(false);
    when(event.getKey()).thenReturn(key);
    when(event.getRawNewValue()).thenReturn(value);
    when(region.getCancelCriterion()).thenReturn(cancelCriterion);
    when(region.getAttributes()).thenReturn(regionAttributes);
  }

  @Test
  public void serverPutWillCheckPutIfAbsentResult() {
    Object result = new Object();
    operation = Operation.PUT_IF_ABSENT;
    when(event.getOperation()).thenReturn(operation);
    when(event.isCreate()).thenReturn(true);
    when(serverRegionProxy.put(key, value, null, event, operation, true, null, null, true))
        .thenReturn(result);
    doCallRealMethod().when(region).serverPut(event, true, null);

    region.serverPut(event, true, null);

    verify(region).checkPutIfAbsentResult(event, value, result);
  }

  @Test
  public void checkPutIfAbsentResultSucceedsIfResultIsNull() {
    Object result = null;
    doCallRealMethod().when(region).checkPutIfAbsentResult(event, value, result);

    region.checkPutIfAbsentResult(event, value, result);

    verify(event, never()).hasRetried();
  }

  @Test(expected = EntryNotFoundException.class)
  public void checkPutIfAbsentResultThrowsIfResultNotNullAndEventHasNotRetried() {
    Object result = new Object();
    when(event.hasRetried()).thenReturn(false);
    doCallRealMethod().when(region).checkPutIfAbsentResult(event, value, result);

    region.checkPutIfAbsentResult(event, value, result);
  }

  @Test(expected = EntryNotFoundException.class)
  public void checkPutIfAbsentResultThrowsIfEventHasRetriedButResultNotHaveSameValue() {
    Object result = new Object();
    when(event.hasRetried()).thenReturn(true);
    when(region.putIfAbsentResultHasSameValue(true, value, result)).thenReturn(false);
    doCallRealMethod().when(region).checkPutIfAbsentResult(event, value, result);

    region.checkPutIfAbsentResult(event, value, result);
  }

  @Test
  public void checkPutIfAbsentResultSucceedsIfEventHasRetriedAndResultHasSameValue() {
    Object result = new Object();
    when(event.hasRetried()).thenReturn(true);
    when(region.putIfAbsentResultHasSameValue(true, value, result)).thenReturn(true);
    doCallRealMethod().when(region).checkPutIfAbsentResult(event, value, result);

    region.checkPutIfAbsentResult(event, value, result);

    verify(event).hasRetried();
    verify(region).putIfAbsentResultHasSameValue(true, value, result);
  }

  @Test
  public void putIfAbsentResultHasSameValueReturnTrueIfResultIsInvalidTokenAndValueToBePutIsNull() {
    when(region.putIfAbsentResultHasSameValue(true, null, Token.INVALID)).thenCallRealMethod();

    assertThat(region.putIfAbsentResultHasSameValue(true, null, Token.INVALID)).isTrue();
  }

  @Test
  public void putIfAbsentResultHasSameValueReturnFalseIfResultIsInvalidTokenAndValueToBePutIsNotNull() {
    when(region.putIfAbsentResultHasSameValue(true, value, Token.INVALID)).thenCallRealMethod();

    assertThat(region.putIfAbsentResultHasSameValue(true, value, Token.INVALID)).isFalse();
  }

  @Test
  public void putIfAbsentResultHasSameValueReturnTrueIfResultHasSameValue() {
    Object result = "value";
    when(region.putIfAbsentResultHasSameValue(true, value, result)).thenCallRealMethod();

    assertThat(region.putIfAbsentResultHasSameValue(true, value, result)).isTrue();
    verify(region, never()).getAttributes();
  }

  @Test
  public void putIfAbsentResultHasSameValueReturnFalseIfResultDoesNotHaveSameValue() {
    Object result = "differentValue";
    when(region.putIfAbsentResultHasSameValue(true, value, result)).thenCallRealMethod();

    assertThat(region.putIfAbsentResultHasSameValue(true, value, result)).isFalse();
    verify(region, never()).getAttributes();
  }

  @Test
  public void putIfAbsentResultHasSameValueChecksRegionAttributesIfNotFromClient() {
    Object result = "value";
    when(region.putIfAbsentResultHasSameValue(false, value, result)).thenCallRealMethod();

    assertThat(region.putIfAbsentResultHasSameValue(false, value, result)).isTrue();
    verify(region).getAttributes();
  }

  @Test
  public void putIfAbsentResultHasSameValueReturnFalseIfResultDoesNotHaveSameValueAndNotFromClient() {
    Object oldValue = "differentValue";
    Object result = new VMCachedDeserializable(EntryEventImpl.serialize(oldValue));
    when(region.putIfAbsentResultHasSameValue(false, value, result)).thenCallRealMethod();

    assertThat(region.putIfAbsentResultHasSameValue(false, value, result)).isFalse();
    verify(region).getAttributes();
  }

  @Test
  public void putIfAbsentResultHasSameValueReturnTrueIfResultHasSameValueAndNotFromClient() {
    Object result = new VMCachedDeserializable(EntryEventImpl.serialize(value));
    when(region.putIfAbsentResultHasSameValue(false, value, result)).thenCallRealMethod();

    assertThat(region.putIfAbsentResultHasSameValue(false, value, result)).isTrue();
    verify(region).getAttributes();
  }

  @Test
  public void bridgePutIfAbsentResultHasSameValueCanCheckValueForObject() {
    Object result = "value";
    byte[] valueToBePut = EntryEventImpl.serialize(value);
    when(region.bridgePutIfAbsentResultHasSameValue(valueToBePut, true, result))
        .thenCallRealMethod();

    assertThat(region.bridgePutIfAbsentResultHasSameValue(valueToBePut, true, result)).isTrue();
    verify(region).getAttributes();
  }

  @Test
  public void bridgePutIfAbsentResultHasSameValueCanCheckValueForNonObjectByteArray() {
    byte[] valueToBePut = {0, 1, 2, 3};
    Object result = new VMCachedDeserializable(EntryEventImpl.serialize(valueToBePut));
    when(region.bridgePutIfAbsentResultHasSameValue(valueToBePut, false, result))
        .thenCallRealMethod();

    assertThat(region.bridgePutIfAbsentResultHasSameValue(valueToBePut, false, result)).isTrue();
    verify(region).getAttributes();
  }

  @Test
  public void bridgePutIfAbsentResultHasSameValueCanCheckValueForIntArray() {
    int[] newValue = {0, 1, 2, 3};
    byte[] valueToBePut = EntryEventImpl.serialize(newValue);
    Object result = newValue;
    when(region.bridgePutIfAbsentResultHasSameValue(valueToBePut, true, result))
        .thenCallRealMethod();

    assertThat(region.bridgePutIfAbsentResultHasSameValue(valueToBePut, true, result)).isTrue();
    verify(region).getAttributes();
  }

  @Test
  public void bridgePutIfAbsentResultHasSameValueCanCheckValueForArrayOfArray() {
    String[] array1 = {"0", "1", "2"};
    String[] array2 = {"3", "4", "5"};
    String[] array3 = {"7"};
    String[][] newValue = {array1, array2, array3};
    byte[] valueToBePut = EntryEventImpl.serialize(newValue);
    Object result = new VMCachedDeserializable(EntryEventImpl.serialize(newValue));
    when(region.bridgePutIfAbsentResultHasSameValue(valueToBePut, true, result))
        .thenCallRealMethod();

    assertThat(region.bridgePutIfAbsentResultHasSameValue(valueToBePut, true, result)).isTrue();
    verify(region).getAttributes();
  }

  @Test
  public void bridgePutIfAbsentResultHasSameValueCanCheckDifferentValuesForArrayOfArray() {
    String[] array1 = {"0", "1", "2"};
    String[] array2 = {"3", "4", "5"};
    String[] array3 = {"7"};
    String[] array4 = {"8"};
    String[][] newValue = {array1, array2, array3};
    String[][] returnedValue = {array1, array2, array4};
    byte[] valueToBePut = EntryEventImpl.serialize(newValue);
    Object result = new VMCachedDeserializable(EntryEventImpl.serialize(returnedValue));
    when(region.bridgePutIfAbsentResultHasSameValue(valueToBePut, true, result))
        .thenCallRealMethod();

    assertThat(region.bridgePutIfAbsentResultHasSameValue(valueToBePut, true, result)).isFalse();
    verify(region).getAttributes();
  }

}
