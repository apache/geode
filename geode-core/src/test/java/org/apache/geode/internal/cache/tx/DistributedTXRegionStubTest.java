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

package org.apache.geode.internal.cache.tx;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.TransactionDataNodeHasDepartedException;
import org.apache.geode.cache.TransactionDataNotColocatedException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.DistributedPutAllOperation;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.DistributedRemoveAllOperation;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.KeyInfo;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RemoteOperationException;
import org.apache.geode.internal.cache.TXStateStub;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;

public class DistributedTXRegionStubTest {

  private TXStateStub txStateStub;
  private DistributedRegion distributedRegion;
  private Object expectedObject;
  private EntryEventImpl event;
  private DistributedMember remoteTransactionHost;
  private KeyInfo keyInfo;

  @Before
  public void setup() {
    txStateStub = mock(TXStateStub.class);
    distributedRegion = mock(DistributedRegion.class, RETURNS_DEEP_STUBS);
    event = mock(EntryEventImpl.class);
    expectedObject = new Object();
    remoteTransactionHost = mock(InternalDistributedMember.class);
    keyInfo = mock(KeyInfo.class);

    when(txStateStub.getTarget()).thenReturn(remoteTransactionHost);
    when(event.getEventId()).thenReturn(mock(EventID.class));
    when(event.getRegion()).thenReturn(distributedRegion);
    when(keyInfo.getKey()).thenReturn(expectedObject);
  }

  @Test
  public void destroyExistingEntryTranslatesRegionDestroyedException() throws Exception {
    DistributedTXRegionStub stub = spy(new DistributedTXRegionStub(txStateStub, distributedRegion));

    when(event.getRegion()).thenReturn(distributedRegion);

    doThrow(RegionDestroyedException.class).when(stub).sendRemoteDestroyMessage(event,
        expectedObject);

    Throwable caughtException =
        catchThrowable(() -> stub.destroyExistingEntry(event, true, expectedObject));

    assertThat(caughtException).isInstanceOf(TransactionDataNotColocatedException.class);
  }

  @Test
  public void destroyExistingEntryTranslatesRemoteOperationException() throws Exception {
    DistributedTXRegionStub stub = spy(new DistributedTXRegionStub(txStateStub, distributedRegion));
    when(event.getRegion()).thenReturn(distributedRegion);

    doThrow(RemoteOperationException.class).when(stub).sendRemoteDestroyMessage(event,
        expectedObject);

    Throwable caughtException =
        catchThrowable(() -> stub.destroyExistingEntry(event, true, expectedObject));

    assertThat(caughtException).isInstanceOf(TransactionDataNodeHasDepartedException.class);
  }

  @Test
  public void getEntryReturnsNullIfEntryNotFoundExceptionIsThrown() throws Exception {
    DistributedTXRegionStub stub = spy(new DistributedTXRegionStub(txStateStub, distributedRegion));

    doThrow(EntryNotFoundException.class).when(stub).sendRemoteFetchEntryMessage(
        any(InternalDistributedMember.class), any(DistributedRegion.class), any(Object.class));

    Object returned = stub.getEntry(keyInfo, true);

    assertThat(returned).isNull();
  }

  @Test
  public void getEntryTranslatesRegionDestroyedException() throws Exception {
    DistributedTXRegionStub stub = spy(new DistributedTXRegionStub(txStateStub, distributedRegion));

    doThrow(RegionDestroyedException.class).when(stub).sendRemoteFetchEntryMessage(
        any(InternalDistributedMember.class), any(DistributedRegion.class), any(Object.class));

    Throwable caughtException = catchThrowable(() -> stub.getEntry(keyInfo, true));

    assertThat(caughtException).isInstanceOf(TransactionDataNotColocatedException.class);
  }

  @Test
  public void getEntryTranslatesRemoteOperationException() throws Exception {
    DistributedTXRegionStub stub = spy(new DistributedTXRegionStub(txStateStub, distributedRegion));

    doThrow(RemoteOperationException.class).when(stub).sendRemoteFetchEntryMessage(
        any(InternalDistributedMember.class), any(LocalRegion.class), any(Object.class));

    Throwable caughtException = catchThrowable(() -> stub.getEntry(keyInfo, true));

    assertThat(caughtException).isInstanceOf(TransactionDataNodeHasDepartedException.class);
  }

  @Test
  public void getEntryThrowsTransactionException() throws Exception {
    DistributedTXRegionStub stub = spy(new DistributedTXRegionStub(txStateStub, distributedRegion));

    doThrow(TransactionException.class).when(stub).sendRemoteFetchEntryMessage(
        any(InternalDistributedMember.class), any(LocalRegion.class), any(Object.class));

    Throwable caughtException = catchThrowable(() -> stub.getEntry(keyInfo, true));

    assertThat(caughtException).isInstanceOf(TransactionException.class);
  }

  @Test
  public void invalidateExistingEntryTranslatesRegionDestroyedException() throws Exception {
    DistributedTXRegionStub stub = spy(new DistributedTXRegionStub(txStateStub, distributedRegion));

    doThrow(RegionDestroyedException.class).when(stub)
        .sendRemoteInvalidateMessage(any(DistributedMember.class), any(EntryEventImpl.class));

    Throwable caughtException =
        catchThrowable(() -> stub.invalidateExistingEntry(event, false, false));

    assertThat(caughtException).isInstanceOf(TransactionDataNotColocatedException.class);
  }

  @Test
  public void invalidateExistingEntryTranslatesRemoteOperationException() throws Exception {
    DistributedTXRegionStub stub = spy(new DistributedTXRegionStub(txStateStub, distributedRegion));

    doThrow(RemoteOperationException.class).when(stub)
        .sendRemoteInvalidateMessage(any(DistributedMember.class), any(EntryEventImpl.class));

    Throwable caughtException =
        catchThrowable(() -> stub.invalidateExistingEntry(event, false, false));

    assertThat(caughtException).isInstanceOf(TransactionDataNodeHasDepartedException.class);
  }

  @Test
  public void containsKeyTranslatesRegionDestroyedException() throws Exception {
    DistributedTXRegionStub stub = spy(new DistributedTXRegionStub(txStateStub, distributedRegion));

    doThrow(RegionDestroyedException.class).when(stub)
        .sendRemoteContainsKeyValueMessage(any(InternalDistributedMember.class), any(Object.class),
            any(boolean.class));

    Throwable caughtException = catchThrowable(() -> stub.containsKey(keyInfo));

    assertThat(caughtException).isInstanceOf(TransactionDataNotColocatedException.class);
  }

  @Test
  public void containsKeyTranslatesRemoteOperationException() throws Exception {
    DistributedTXRegionStub stub = spy(new DistributedTXRegionStub(txStateStub, distributedRegion));

    doThrow(RemoteOperationException.class).when(stub)
        .sendRemoteContainsKeyValueMessage(any(InternalDistributedMember.class), any(Object.class),
            any(boolean.class));

    Throwable caughtException =
        catchThrowable(() -> stub.containsKey(keyInfo));

    assertThat(caughtException).isInstanceOf(TransactionDataNodeHasDepartedException.class);
  }

  @Test
  public void containsValueForKeyTranslatesRegionDestroyedException() throws Exception {
    DistributedTXRegionStub stub = spy(new DistributedTXRegionStub(txStateStub, distributedRegion));

    doThrow(RegionDestroyedException.class).when(stub)
        .sendRemoteContainsKeyValueMessage(any(InternalDistributedMember.class), any(Object.class),
            any(boolean.class));

    Throwable caughtException = catchThrowable(() -> stub.containsValueForKey(keyInfo));

    assertThat(caughtException).isInstanceOf(TransactionDataNotColocatedException.class);
  }

  @Test
  public void containsValueForKeyTranslatesRemoteOperationException() throws Exception {
    DistributedTXRegionStub stub = spy(new DistributedTXRegionStub(txStateStub, distributedRegion));

    doThrow(RemoteOperationException.class).when(stub)
        .sendRemoteContainsKeyValueMessage(any(InternalDistributedMember.class), any(Object.class),
            any(boolean.class));

    Throwable caughtException = catchThrowable(() -> stub.containsValueForKey(keyInfo));

    assertThat(caughtException).isInstanceOf(TransactionDataNodeHasDepartedException.class);
  }

  @Test
  public void findObjectTranslatesRegionDestroyedException() throws Exception {
    DistributedTXRegionStub stub = spy(new DistributedTXRegionStub(txStateStub, distributedRegion));
    ClientProxyMembershipID requestingClient = mock(ClientProxyMembershipID.class);

    Object callbackArg = new Object();
    when(keyInfo.getCallbackArg()).thenReturn(callbackArg);

    doThrow(RegionDestroyedException.class).when(stub).sendRemoteGetMessage(
        (InternalDistributedMember) remoteTransactionHost, expectedObject, callbackArg,
        requestingClient);

    Throwable caughtException = catchThrowable(() -> stub.findObject(keyInfo, false, false,
        expectedObject, false, requestingClient, event));

    assertThat(caughtException).isInstanceOf(TransactionDataNotColocatedException.class);
  }

  @Test
  public void findObjectTranslatesRemoteOperationException() throws Exception {
    DistributedTXRegionStub stub = spy(new DistributedTXRegionStub(txStateStub, distributedRegion));
    ClientProxyMembershipID requestingClient = mock(ClientProxyMembershipID.class);

    Object callbackArg = new Object();
    when(keyInfo.getCallbackArg()).thenReturn(callbackArg);

    doThrow(RemoteOperationException.class).when(stub).sendRemoteGetMessage(
        (InternalDistributedMember) remoteTransactionHost, expectedObject, callbackArg,
        requestingClient);

    Throwable caughtException = catchThrowable(() -> stub.findObject(keyInfo, false, false,
        expectedObject, false, requestingClient, event));

    assertThat(caughtException).isInstanceOf(TransactionDataNodeHasDepartedException.class);
  }

  @Test
  public void putEntryTranslatesRegionDestroyedException() throws Exception {
    DistributedTXRegionStub stub = spy(new DistributedTXRegionStub(txStateStub, distributedRegion));

    doThrow(RegionDestroyedException.class).when(stub).txSendRemotePutMessage(remoteTransactionHost,
        distributedRegion, event, 0, true, false, expectedObject, true);

    Throwable caughtException =
        catchThrowable(() -> stub.putEntry(event, true, false, expectedObject, true, 0, false));

    assertThat(caughtException).isInstanceOf(TransactionDataNotColocatedException.class);
  }

  @Test
  public void putEntryTranslatesRemoteOperationException() throws Exception {
    DistributedTXRegionStub stub = spy(new DistributedTXRegionStub(txStateStub, distributedRegion));

    doThrow(RemoteOperationException.class).when(stub).txSendRemotePutMessage(remoteTransactionHost,
        distributedRegion, event, 0, true, false, expectedObject, true);

    Throwable caughtException =
        catchThrowable(() -> stub.putEntry(event, true, false, expectedObject, true, 0, false));

    assertThat(caughtException).isInstanceOf(TransactionDataNodeHasDepartedException.class);
  }

  @Test
  public void postPutAllTranslatesRegionDestroyedException() throws Exception {
    DistributedTXRegionStub stub = spy(new DistributedTXRegionStub(txStateStub, distributedRegion));
    DistributedPutAllOperation putAllOp = mock(DistributedPutAllOperation.class);
    VersionedObjectList successfulPuts = mock(VersionedObjectList.class);
    DistributedPutAllOperation.PutAllEntryData[] entries =
        new DistributedPutAllOperation.PutAllEntryData[1];

    when(putAllOp.getPutAllEntryData()).thenReturn(entries);
    when(putAllOp.getBaseEvent()).thenReturn(event);

    doThrow(RegionDestroyedException.class).when(stub)
        .sendRemotePutAllMessage(remoteTransactionHost, event, entries, entries.length);

    Throwable caughtException =
        catchThrowable(() -> stub.postPutAll(putAllOp, successfulPuts, distributedRegion));

    assertThat(caughtException).isInstanceOf(TransactionDataNotColocatedException.class);
  }

  @Test
  public void postPutAllTranslatesRemoteOperationException() throws Exception {
    DistributedTXRegionStub stub = spy(new DistributedTXRegionStub(txStateStub, distributedRegion));
    DistributedPutAllOperation putAllOp = mock(DistributedPutAllOperation.class);
    VersionedObjectList successfulPuts = mock(VersionedObjectList.class);
    DistributedPutAllOperation.PutAllEntryData[] entries =
        new DistributedPutAllOperation.PutAllEntryData[1];

    when(putAllOp.getPutAllEntryData()).thenReturn(entries);
    when(putAllOp.getBaseEvent()).thenReturn(event);

    doThrow(RemoteOperationException.class).when(stub)
        .sendRemotePutAllMessage(remoteTransactionHost, event, entries, entries.length);

    Throwable caughtException =
        catchThrowable(() -> stub.postPutAll(putAllOp, successfulPuts, distributedRegion));

    assertThat(caughtException).isInstanceOf(TransactionDataNodeHasDepartedException.class);
  }

  @Test
  public void postRemoveAllTranslatesRegionDestroyedException() throws Exception {
    DistributedTXRegionStub stub = spy(new DistributedTXRegionStub(txStateStub, distributedRegion));
    DistributedRemoveAllOperation removeAllOp = mock(DistributedRemoveAllOperation.class);
    VersionedObjectList successfulPuts = mock(VersionedObjectList.class);
    DistributedRemoveAllOperation.RemoveAllEntryData[] entries =
        new DistributedRemoveAllOperation.RemoveAllEntryData[1];

    when(removeAllOp.getRemoveAllEntryData()).thenReturn(entries);
    when(removeAllOp.getBaseEvent()).thenReturn(event);

    doThrow(RegionDestroyedException.class).when(stub)
        .sendRemoteRemoveAllMessage(remoteTransactionHost, event, entries, entries.length);

    Throwable caughtException =
        catchThrowable(() -> stub.postRemoveAll(removeAllOp, successfulPuts, distributedRegion));

    assertThat(caughtException).isInstanceOf(TransactionDataNotColocatedException.class);
  }

  @Test
  public void postRemoveAllTranslatesRemoteOperationException() throws Exception {
    DistributedTXRegionStub stub = spy(new DistributedTXRegionStub(txStateStub, distributedRegion));
    DistributedRemoveAllOperation removeAllOp = mock(DistributedRemoveAllOperation.class);
    VersionedObjectList successfulPuts = mock(VersionedObjectList.class);
    DistributedRemoveAllOperation.RemoveAllEntryData[] entries =
        new DistributedRemoveAllOperation.RemoveAllEntryData[1];

    when(removeAllOp.getRemoveAllEntryData()).thenReturn(entries);
    when(removeAllOp.getBaseEvent()).thenReturn(event);

    doThrow(RemoteOperationException.class).when(stub)
        .sendRemoteRemoveAllMessage(remoteTransactionHost, event, entries, entries.length);

    Throwable caughtException =
        catchThrowable(() -> stub.postRemoveAll(removeAllOp, successfulPuts, distributedRegion));

    assertThat(caughtException).isInstanceOf(TransactionDataNodeHasDepartedException.class);
  }

}
