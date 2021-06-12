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
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.TransactionDataNodeHasDepartedException;
import org.apache.geode.cache.TransactionDataRebalancedException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.BucketNotFoundException;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.InternalDataView;
import org.apache.geode.internal.cache.KeyInfo;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.TXStateStub;

public class PartitionedTXRegionStubTest {
  private TXStateStub txStateStub;
  private PartitionedRegion partitionedRegion;
  private EntryEventImpl event;
  private Object expectedObject;
  private TransactionException expectedException;
  private DistributedMember remoteTransactionHost;
  private KeyInfo keyInfo;
  private Object key;

  @Before
  public void setup() {
    txStateStub = mock(TXStateStub.class);
    partitionedRegion = mock(PartitionedRegion.class, RETURNS_DEEP_STUBS);
    event = mock(EntryEventImpl.class);
    expectedObject = new Object();
    expectedException = new TransactionException();
    remoteTransactionHost = mock(InternalDistributedMember.class);
    keyInfo = mock(KeyInfo.class);
    key = new Object();
    when(txStateStub.getTarget()).thenReturn(remoteTransactionHost);
    when(event.getKeyInfo()).thenReturn(keyInfo);
    when(keyInfo.getKey()).thenReturn(key);
    when(keyInfo.getBucketId()).thenReturn(1);
  }

  @Test
  public void destroyExistingEntryTracksBucketForTx() {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    when(event.getRegion()).thenReturn(partitionedRegion);

    stub.destroyExistingEntry(event, true, expectedObject);

    verify(stub).trackBucketForTx(keyInfo);
  }

  @Test
  public void destroyExistingEntryThrowsTransactionExceptionFromRemoteHost() throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    when(event.getRegion()).thenReturn(partitionedRegion);
    doThrow(expectedException).when(partitionedRegion).destroyRemotely(remoteTransactionHost, 1,
        event, expectedObject);

    Throwable caughtException = catchThrowable(() -> stub.destroyExistingEntry(event, true,
        expectedObject));

    assertThat(caughtException).isSameAs(expectedException);
    verify(stub, never()).trackBucketForTx(keyInfo);
  }

  @Test
  public void destroyExistingEntryThrowsTransactionDataRebalancedExceptionIfIsBucketNotFoundException()
      throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    when(event.getRegion()).thenReturn(partitionedRegion);
    ForceReattemptException forceReattemptException = new ForceReattemptException("Test FRE");
    doReturn(true).when(stub).isBucketNotFoundException(forceReattemptException);
    doNothing().when(stub).waitToRetry();
    doThrow(forceReattemptException).when(partitionedRegion).destroyRemotely(remoteTransactionHost,
        1, event, expectedObject);

    Throwable caughtException = catchThrowable(() -> stub.destroyExistingEntry(event, false,
        expectedObject));

    assertThat(caughtException).isInstanceOf(TransactionDataRebalancedException.class);
    verify(stub, never()).trackBucketForTx(keyInfo);
  }

  @Test
  public void destroyExistingEntryThrowsTransactionDataNodeHasDepartedExceptionIfIsNotBucketNotFoundException()
      throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    when(event.getRegion()).thenReturn(partitionedRegion);
    ForceReattemptException forceReattemptException = new ForceReattemptException("Test FRE");
    doReturn(false).when(stub).isBucketNotFoundException(forceReattemptException);
    doNothing().when(stub).waitToRetry();
    doThrow(forceReattemptException).when(partitionedRegion).destroyRemotely(remoteTransactionHost,
        1, event, expectedObject);

    Throwable caughtException = catchThrowable(() -> stub.destroyExistingEntry(event, true,
        expectedObject));

    assertThat(caughtException).isInstanceOf(TransactionDataNodeHasDepartedException.class);
    verify(stub, never()).trackBucketForTx(keyInfo);
  }

  @Test
  public void getEntryForIteratorReturnsEntryGotFromTransactionHost() throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    EntrySnapshot entry = mock(EntrySnapshot.class);

    when(event.getRegion()).thenReturn(partitionedRegion);
    when(partitionedRegion.getBucketPrimary(1))
        .thenReturn((InternalDistributedMember) remoteTransactionHost);
    when(partitionedRegion.getEntryRemotely((InternalDistributedMember) remoteTransactionHost, 1,
        key, false, true)).thenReturn(entry);

    assertThat(stub.getEntryForIterator(keyInfo, true)).isEqualTo(entry);
    verify(stub).trackBucketForTx(keyInfo);
  }

  @Test
  public void getEntryForIteratorReturnsEntryGotFromAnyDataHosts() {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    EntrySnapshot entry = mock(EntrySnapshot.class);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    InternalDataView dataView = mock(InternalDataView.class);

    when(event.getRegion()).thenReturn(partitionedRegion);
    when(partitionedRegion.getBucketPrimary(keyInfo.getBucketId())).thenReturn(member);
    when(partitionedRegion.getSharedDataView()).thenReturn(dataView);
    when(dataView.getEntry(keyInfo, partitionedRegion, true)).thenReturn(entry);

    assertThat(stub.getEntryForIterator(keyInfo, true)).isEqualTo(entry);
  }

  @Test
  public void getEntryReturnsEntryGotFromRemote() throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    EntrySnapshot entry = mock(EntrySnapshot.class);
    when(event.getRegion()).thenReturn(partitionedRegion);
    when(partitionedRegion.getEntryRemotely((InternalDistributedMember) remoteTransactionHost, 1,
        key, false, true)).thenReturn((entry));

    assertThat(stub.getEntry(keyInfo, true)).isEqualTo(entry);
    verify(stub).trackBucketForTx(keyInfo);
  }

  @Test
  public void isBucketNotFoundExceptionReturnsTrue() {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    BucketNotFoundException ex = new BucketNotFoundException("Test BNFE");

    assertThat(stub.isBucketNotFoundException(ex)).isEqualTo(true);
  }

  @Test
  public void isBucketNotFoundExceptionReturnsFalse() {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    ForceReattemptException ex = new ForceReattemptException("Test FRE");

    assertThat(stub.isBucketNotFoundException(ex)).isEqualTo(false);
  }

  @Test
  public void isBucketNotFoundExceptionReturnsTrueIfCausedByBucketNotFoundException() {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));

    Throwable cause = new BucketNotFoundException("Test BNFE");
    ForceReattemptException ex = new ForceReattemptException("Test FRE", cause);

    assertThat(stub.isBucketNotFoundException(ex)).isEqualTo(true);
  }

  @Test
  public void getEntryThrowsTransactionExceptionFromRemoteHost() throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    when(event.getRegion()).thenReturn(partitionedRegion);
    doThrow(expectedException).when(partitionedRegion)
        .getEntryRemotely((InternalDistributedMember) remoteTransactionHost, 1, key, false, true);

    Throwable caughtException = catchThrowable(() -> stub.getEntry(keyInfo, true));

    assertThat(caughtException).isSameAs(expectedException);
    verify(stub, never()).trackBucketForTx(keyInfo);
  }

  @Test
  public void getEntryThrowsTransactionDataRebalancedExceptionIfIsBucketNotFoundException()
      throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    when(event.getRegion()).thenReturn(partitionedRegion);
    ForceReattemptException forceReattemptException = new ForceReattemptException("Test FRE");
    doReturn(true).when(stub).isBucketNotFoundException(forceReattemptException);
    doNothing().when(stub).waitToRetry();
    doThrow(forceReattemptException).when(partitionedRegion)
        .getEntryRemotely((InternalDistributedMember) remoteTransactionHost, 1, key, false, true);

    Throwable caughtException = catchThrowable(() -> stub.getEntry(keyInfo, true));

    assertThat(caughtException).isInstanceOf(TransactionDataRebalancedException.class);
    verify(stub, never()).trackBucketForTx(keyInfo);
  }

  @Test
  public void getEntryThrowsTransactionDataNodeHasDepartedExceptionIfIsNotBucketNotFoundException()
      throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    when(event.getRegion()).thenReturn(partitionedRegion);

    ForceReattemptException forceReattemptException = new ForceReattemptException("Test FRE");
    doReturn(false).when(stub).isBucketNotFoundException(forceReattemptException);
    doNothing().when(stub).waitToRetry();
    doThrow(forceReattemptException).when(partitionedRegion)
        .getEntryRemotely((InternalDistributedMember) remoteTransactionHost, 1, key, false, false);

    Throwable caughtException = catchThrowable(() -> stub.getEntry(keyInfo, false));

    assertThat(caughtException).isInstanceOf(TransactionDataNodeHasDepartedException.class);
    verify(stub, never()).trackBucketForTx(keyInfo);
  }

  @Test
  public void invalidateExistingEntryTracksBucketForTx() {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    when(event.getRegion()).thenReturn(partitionedRegion);

    stub.invalidateExistingEntry(event, true, false);

    verify(stub).trackBucketForTx(keyInfo);
  }

  @Test
  public void invalidateExistingEntryThrowsTransactionExceptionFromRemoteHost() throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    when(event.getRegion()).thenReturn(partitionedRegion);
    when(keyInfo.getBucketId()).thenReturn(1);
    doThrow(expectedException).when(partitionedRegion).invalidateRemotely(remoteTransactionHost, 1,
        event);

    Throwable caughtException =
        catchThrowable(() -> stub.invalidateExistingEntry(event, false, false));

    assertThat(caughtException).isSameAs(expectedException);
    verify(stub, never()).trackBucketForTx(keyInfo);
  }

  @Test
  public void invalidateExistingEntryThrowsTransactionDataRebalancedExceptionIfIsBucketNotFoundException()
      throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    when(event.getRegion()).thenReturn(partitionedRegion);
    when(keyInfo.getBucketId()).thenReturn(1);
    ForceReattemptException forceReattemptException = new ForceReattemptException("Test FRE");
    doReturn(true).when(stub).isBucketNotFoundException(forceReattemptException);
    doNothing().when(stub).waitToRetry();
    doThrow(forceReattemptException).when(partitionedRegion)
        .invalidateRemotely(remoteTransactionHost, 1, event);

    Throwable caughtException =
        catchThrowable(() -> stub.invalidateExistingEntry(event, false, false));

    assertThat(caughtException).isInstanceOf(TransactionDataRebalancedException.class);
    verify(stub, never()).trackBucketForTx(keyInfo);
  }

  @Test
  public void invalidateExistingEntryThrowsTransactionDataNodeHasDepartedExceptionIfIsNotBucketNotFoundException()
      throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    when(event.getRegion()).thenReturn(partitionedRegion);
    when(keyInfo.getBucketId()).thenReturn(1);
    ForceReattemptException forceReattemptException = new ForceReattemptException("Test FRE");
    doReturn(false).when(stub).isBucketNotFoundException(forceReattemptException);
    doNothing().when(stub).waitToRetry();
    doThrow(forceReattemptException).when(partitionedRegion)
        .invalidateRemotely(remoteTransactionHost, 1, event);

    Throwable caughtException =
        catchThrowable(() -> stub.invalidateExistingEntry(event, false, false));

    assertThat(caughtException).isInstanceOf(TransactionDataNodeHasDepartedException.class);
    verify(stub, never()).trackBucketForTx(keyInfo);
  }

  @Test
  public void containsKeyReturnsTrueIfContainsKeyRemotelyReturnsTrue() throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    when(event.getRegion()).thenReturn(partitionedRegion);
    when(partitionedRegion.containsKeyRemotely((InternalDistributedMember) remoteTransactionHost, 1,
        key)).thenReturn(true);

    assertThat(stub.containsKey(keyInfo)).isTrue();
    verify(stub).trackBucketForTx(keyInfo);
  }

  @Test
  public void containsKeyReturnsFalseIfContainsKeyRemotelyReturnsFalse() throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    when(event.getRegion()).thenReturn(partitionedRegion);
    when(partitionedRegion.containsKeyRemotely((InternalDistributedMember) remoteTransactionHost, 1,
        key)).thenReturn(false);

    assertThat(stub.containsKey(keyInfo)).isFalse();
    verify(stub).trackBucketForTx(keyInfo);
  }

  @Test
  public void containsKeyThrowsTransactionExceptionFromRemoteHost() throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    when(event.getRegion()).thenReturn(partitionedRegion);
    when(partitionedRegion.containsKeyRemotely((InternalDistributedMember) remoteTransactionHost, 1,
        key)).thenThrow(expectedException);

    Throwable caughtException = catchThrowable(() -> stub.containsKey(keyInfo));

    assertThat(caughtException).isSameAs(expectedException);
    verify(stub, never()).trackBucketForTx(keyInfo);
  }

  @Test
  public void containsKeyThrowsTransactionExceptionIfIsBucketNotFoundException()
      throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    when(event.getRegion()).thenReturn(partitionedRegion);
    ForceReattemptException forceReattemptException = new ForceReattemptException("Test FRE");
    doReturn(true).when(stub).isBucketNotFoundException(forceReattemptException);
    when(partitionedRegion.containsKeyRemotely((InternalDistributedMember) remoteTransactionHost, 1,
        key)).thenThrow(forceReattemptException);
    doReturn(expectedException).when(stub).getTransactionException(keyInfo,
        forceReattemptException);

    Throwable caughtException = catchThrowable(() -> stub.containsKey(keyInfo));

    assertThat(caughtException).isInstanceOf(TransactionException.class);
    verify(stub).getTransactionException(keyInfo, forceReattemptException);
    verify(stub, never()).trackBucketForTx(keyInfo);
  }

  @Test
  public void containsKeyThrowsTransactionDataNodeHasDepartedExceptionIfIsNotBucketNotFoundException()
      throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    when(event.getRegion()).thenReturn(partitionedRegion);

    ForceReattemptException forceReattemptException = new ForceReattemptException("Test FRE");
    doNothing().when(stub).waitToRetry();
    doReturn(false).when(stub).isBucketNotFoundException(forceReattemptException);
    when(partitionedRegion.containsKeyRemotely((InternalDistributedMember) remoteTransactionHost, 1,
        key)).thenThrow(forceReattemptException);

    Throwable caughtException = catchThrowable(() -> stub.containsKey(keyInfo));

    assertThat(caughtException).isInstanceOf(TransactionDataNodeHasDepartedException.class);
    verify(stub, never()).trackBucketForTx(keyInfo);
  }

  @Test
  public void containsValueForKeyReturnsTrueIfContainsKeyRemotelyReturnsTrue() throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    when(event.getRegion()).thenReturn(partitionedRegion);
    when(partitionedRegion
        .containsValueForKeyRemotely((InternalDistributedMember) remoteTransactionHost, 1, key))
            .thenReturn(true);

    assertThat(stub.containsValueForKey(keyInfo)).isTrue();
    verify(stub).trackBucketForTx(keyInfo);
  }

  @Test
  public void containsValueForKeyRemotelyReturnsFalseIfContainsKeyRemotelyReturnsFalse()
      throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    when(event.getRegion()).thenReturn(partitionedRegion);
    when(partitionedRegion
        .containsValueForKeyRemotely((InternalDistributedMember) remoteTransactionHost, 1, key))
            .thenReturn(false);

    assertThat(stub.containsValueForKey(keyInfo)).isFalse();
    verify(stub).trackBucketForTx(keyInfo);
  }

  @Test
  public void containsValueForKeyThrowsTransactionExceptionFromRemoteHost() throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    when(event.getRegion()).thenReturn(partitionedRegion);
    when(partitionedRegion
        .containsValueForKeyRemotely((InternalDistributedMember) remoteTransactionHost, 1, key))
            .thenThrow(expectedException);

    Throwable caughtException = catchThrowable(() -> stub.containsValueForKey(keyInfo));

    assertThat(caughtException).isSameAs(expectedException);
    verify(stub, never()).trackBucketForTx(keyInfo);
  }

  @Test
  public void containsValueForKeyThrowsTransactionExceptionIfIsBucketNotFoundException()
      throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    when(event.getRegion()).thenReturn(partitionedRegion);
    ForceReattemptException forceReattemptException = new ForceReattemptException("Test FRE");
    doReturn(true).when(stub).isBucketNotFoundException(forceReattemptException);
    doReturn(expectedException).when(stub).getTransactionException(keyInfo,
        forceReattemptException);
    when(partitionedRegion
        .containsValueForKeyRemotely((InternalDistributedMember) remoteTransactionHost, 1, key))
            .thenThrow(forceReattemptException);

    Throwable caughtException = catchThrowable(() -> stub.containsValueForKey(keyInfo));

    assertThat(caughtException).isInstanceOf(TransactionException.class);
    verify(stub, never()).trackBucketForTx(keyInfo);
  }

  @Test
  public void containsValueForKeyThrowsTransactionDataNodeHasDepartedExceptionIfIsNotBucketNotFoundException()
      throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    when(event.getRegion()).thenReturn(partitionedRegion);

    ForceReattemptException forceReattemptException = new ForceReattemptException("Test FRE");
    doNothing().when(stub).waitToRetry();
    doReturn(false).when(stub).isBucketNotFoundException(forceReattemptException);
    when(partitionedRegion
        .containsValueForKeyRemotely((InternalDistributedMember) remoteTransactionHost, 1, key))
            .thenThrow(forceReattemptException);

    Throwable caughtException = catchThrowable(() -> stub.containsValueForKey(keyInfo));

    assertThat(caughtException).isInstanceOf(TransactionDataNodeHasDepartedException.class);
    verify(stub, never()).trackBucketForTx(keyInfo);
  }

  @Test
  public void findObjectReturnsObjectFoundFromRemote() throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    EntrySnapshot entry = mock(EntrySnapshot.class);
    when(event.getRegion()).thenReturn(partitionedRegion);
    when(partitionedRegion.getRemotely((InternalDistributedMember) remoteTransactionHost, 1,
        key, null, true, null, event, false)).thenReturn((expectedObject));

    assertThat(stub.findObject(keyInfo, false, false, null, true, null, event))
        .isEqualTo(expectedObject);
    verify(stub).trackBucketForTx(keyInfo);
  }

  @Test
  public void findObjectThrowsTransactionExceptionFromRemoteHost() throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub,
        partitionedRegion));
    when(event.getRegion()).thenReturn(partitionedRegion);
    doThrow(expectedException).when(partitionedRegion).getRemotely(
        (InternalDistributedMember) remoteTransactionHost, 1, key, null, true, null, event, false);

    Throwable caughtException =
        catchThrowable(() -> stub.findObject(keyInfo, false, false, null, true, null, event));

    assertThat(caughtException).isSameAs(expectedException);
    verify(stub, never()).trackBucketForTx(keyInfo);
  }

  @Test
  public void findObjectThrowsTransactionExceptionIfIsBucketNotFoundException()
      throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub,
        partitionedRegion));
    when(event.getRegion()).thenReturn(partitionedRegion);
    ForceReattemptException forceReattemptException = new ForceReattemptException("Test FRE");
    doReturn(true).when(stub).isBucketNotFoundException(forceReattemptException);
    doNothing().when(stub).waitToRetry();
    doThrow(forceReattemptException).when(partitionedRegion)
        .getRemotely((InternalDistributedMember) remoteTransactionHost, 1, key, null, true, null,
            event, false);
    doReturn(expectedException).when(stub).getTransactionException(keyInfo,
        forceReattemptException);

    Throwable caughtException =
        catchThrowable(() -> stub.findObject(keyInfo, false, false, null, true, null, event));

    assertThat(caughtException).isInstanceOf(TransactionException.class);
    verify(stub, never()).trackBucketForTx(keyInfo);
  }

  @Test
  public void findObjectThrowsTransactionExceptionIfIsNotBucketNotFoundException()
      throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub,
        partitionedRegion));
    when(event.getRegion()).thenReturn(partitionedRegion);

    ForceReattemptException forceReattemptException = new ForceReattemptException("Test FRE");
    doReturn(false).when(stub).isBucketNotFoundException(forceReattemptException);
    doNothing().when(stub).waitToRetry();
    doThrow(forceReattemptException).when(partitionedRegion)
        .getRemotely((InternalDistributedMember) remoteTransactionHost, 1, key, null, true, null,
            event, false);
    doReturn(expectedException).when(stub).getTransactionException(keyInfo,
        forceReattemptException);

    Throwable caughtException =
        catchThrowable(() -> stub.findObject(keyInfo, false, false, null, true, null, event));

    assertThat(caughtException).isInstanceOf(TransactionException.class);
    verify(stub, never()).trackBucketForTx(keyInfo);
  }

  @Test
  public void putEntryReturnsTrueIfPutRemotelyReturnsTrue() throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    when(event.getRegion()).thenReturn(partitionedRegion);
    when(partitionedRegion
        .putRemotely((InternalDistributedMember) remoteTransactionHost, event, false, false,
            expectedObject, true))
                .thenReturn(true);

    assertThat(stub.putEntry(event, false, false, expectedObject, true, 1, false)).isTrue();
    verify(stub).trackBucketForTx(keyInfo);
  }

  @Test
  public void putEntryReturnsFalseIfPutRemotelyReturnsFalse()
      throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    when(event.getRegion()).thenReturn(partitionedRegion);
    when(partitionedRegion
        .putRemotely((InternalDistributedMember) remoteTransactionHost, event, false, false,
            expectedObject, true))
                .thenReturn(false);

    assertThat(stub.putEntry(event, false, false, expectedObject, true, 1, false)).isFalse();
    verify(stub).trackBucketForTx(keyInfo);
  }

  @Test
  public void putEntryThrowsTransactionExceptionFromRemoteHost() throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    when(event.getRegion()).thenReturn(partitionedRegion);
    when(partitionedRegion
        .putRemotely((InternalDistributedMember) remoteTransactionHost, event, true, false,
            expectedObject, true))
                .thenThrow(expectedException);

    Throwable caughtException =
        catchThrowable(() -> stub.putEntry(event, true, false, expectedObject, true, 1, false));

    assertThat(caughtException).isSameAs(expectedException);
    verify(stub, never()).trackBucketForTx(keyInfo);
  }

  @Test
  public void putEntryThrowsTransactionExceptionIfIsBucketNotFoundException()
      throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    when(event.getRegion()).thenReturn(partitionedRegion);
    ForceReattemptException forceReattemptException = new ForceReattemptException("Test FRE");
    doReturn(true).when(stub).isBucketNotFoundException(forceReattemptException);
    doReturn(expectedException).when(stub).getTransactionException(keyInfo,
        forceReattemptException);
    when(partitionedRegion
        .putRemotely((InternalDistributedMember) remoteTransactionHost, event, true, false,
            expectedObject, true))
                .thenThrow(forceReattemptException);

    Throwable caughtException =
        catchThrowable(() -> stub.putEntry(event, true, false, expectedObject, true, 1, false));

    assertThat(caughtException).isInstanceOf(TransactionException.class);
    verify(stub, never()).trackBucketForTx(keyInfo);
  }

  @Test
  public void putEntryThrowsTransactionExceptionIfIsNotBucketNotFoundException()
      throws Exception {
    PartitionedTXRegionStub stub = spy(new PartitionedTXRegionStub(txStateStub, partitionedRegion));
    when(event.getRegion()).thenReturn(partitionedRegion);

    ForceReattemptException forceReattemptException = new ForceReattemptException("Test FRE");
    doNothing().when(stub).waitToRetry();
    doReturn(false).when(stub).isBucketNotFoundException(forceReattemptException);
    doReturn(expectedException).when(stub).getTransactionException(keyInfo,
        forceReattemptException);
    when(partitionedRegion
        .putRemotely((InternalDistributedMember) remoteTransactionHost, event, true, false,
            expectedObject, true))
                .thenThrow(forceReattemptException);

    Throwable caughtException =
        catchThrowable(() -> stub.putEntry(event, true, false, expectedObject, true, 1, false));

    assertThat(caughtException).isInstanceOf(TransactionException.class);
    verify(stub, never()).trackBucketForTx(keyInfo);
  }

}
