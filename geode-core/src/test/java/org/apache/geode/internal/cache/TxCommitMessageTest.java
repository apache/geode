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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.junit.Test;
import org.mockito.InOrder;

import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.VersionedDataStream;
import org.apache.geode.internal.cache.versions.VersionSource;

/**
 * Unit tests for TxCommitMessage.
 */
public class TxCommitMessageTest {

  interface VersionedDataOutput extends DataOutput, VersionedDataStream {
  }

  interface VersionedDataInput extends DataInput, VersionedDataStream {
  }

  private InternalDistributedMember createInternalDistributedMember() {
    final InternalDistributedMember mockInternalDistributedMember =
        mock(InternalDistributedMember.class);
    when(mockInternalDistributedMember.getDSFID()).thenReturn(1);
    when(mockInternalDistributedMember.getSerializationVersions()).thenReturn(null);

    return mockInternalDistributedMember;
  }

  private EntryEventImpl createMockEntryEvent(InternalRegion mockInternalRegion) {
    final EntryEventImpl mockEntryEventImpl = mock(EntryEventImpl.class);
    when(mockEntryEventImpl.isLocalInvalid()).thenReturn(false);
    when(mockEntryEventImpl.getRegion()).thenReturn(mockInternalRegion);

    return mockEntryEventImpl;
  }

  private TXEntryState createTxEntryState(InternalRegion mockInternalRegion,
      EntryEventImpl mockEntryEventImpl) {
    final TXState txState = new TXState(null, false);
    final TXRegionState txRegionState = new TXRegionState(mockInternalRegion, txState);
    final TXEntryState txEntryState =
        TXEntryState.getFactory().createEntry(null, null, null, null, txRegionState, false);
    txEntryState.invalidate(mockEntryEventImpl);
    txEntryState.generateEventOffsets(txState);

    return txEntryState;
  }

  private InternalRegion createMockInternalRegion(VersionSource mockVersionSource) {
    final InternalRegion mockInternalRegion = mock(InternalRegion.class);
    when(mockInternalRegion.requiresReliabilityCheck()).thenReturn(false);
    when(mockInternalRegion.getVersionMember()).thenReturn(mockVersionSource);
    when(mockInternalRegion.getFullPath()).thenReturn("/r");
    when(mockInternalRegion.getPersistBackup()).thenReturn(false);
    when(mockInternalRegion.getScope()).thenReturn(Scope.LOCAL);
    when(mockInternalRegion.isEntryEvictionPossible()).thenReturn(false);
    when(mockInternalRegion.isEntryExpiryPossible()).thenReturn(false);
    when(mockInternalRegion.getConcurrencyChecksEnabled()).thenReturn(false);

    return mockInternalRegion;
  }

  @Test
  public void toDataWithShadowKeyPre180Server() throws IOException {
    final VersionedDataOutput mockDataOutput = mock(VersionedDataOutput.class);
    when(mockDataOutput.getVersion()).thenReturn(Version.GEODE_170);
    final InternalDistributedMember mockInternalDistributedMember =
        createInternalDistributedMember();
    final TXId txId = new TXId(mockInternalDistributedMember, 0);
    final TXState txState = new TXState(null, false);
    final TXCommitMessage txCommitMessage = new TXCommitMessage(txId, null, txState);
    final InternalRegion mockInternalRegion =
        createMockInternalRegion(mockInternalDistributedMember);
    txCommitMessage.startRegion(mockInternalRegion, 0);
    final EntryEventImpl mockEntryEventImpl = createMockEntryEvent(mockInternalRegion);
    final TXEntryState txEntryState = createTxEntryState(mockInternalRegion, mockEntryEventImpl);
    txCommitMessage.addOp(null, "key", txEntryState, null);
    txCommitMessage.finishRegionComplete();
    txCommitMessage.toData(mockDataOutput);

    // Asserts
    InOrder dataOutputInOrder = inOrder(mockDataOutput);
    // processor id
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeInt(anyInt());
    // txId.uniqId
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeInt(0);
    // lockId
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeBoolean(false);
    // totalMaxSize
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeInt(0);
    // txState.membershipId
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(-1);
    // txState.baseThreadId, txState.baseSequenceId
    dataOutputInOrder.verify(mockDataOutput, times(2)).writeLong(anyLong());
    // txState.needsLargeModCount
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeBoolean(false);
    // regionsSize
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeInt(1);
    // regionPath: "/r"
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(87);
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeShort(2);
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeBytes("/r");
    // parentRegionPath: null string
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(69);
    // opKeys size
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeInt(1);
    // needsLargeModCount
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeBoolean(false);
    // versionMember
    dataOutputInOrder.verify(mockDataOutput, times(2)).writeByte(1);
    // opKeys[0]
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(87);
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeShort(3);
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeBytes("key");
    // farSideData[0]
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(17);
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(0);
    dataOutputInOrder.verify(mockDataOutput, times(3)).writeByte(41);
    // shadowkey
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeLong(-1L);
    // offset
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeInt(anyInt());
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeBoolean(false);
    // bridgeContext
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(41);
    // farSiders
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(-1);
  }

  @Test
  public void toDataWithoutShadowKeyPre180Client() throws IOException {
    final VersionedDataOutput mockDataOutput = mock(VersionedDataOutput.class);
    when(mockDataOutput.getVersion()).thenReturn(Version.GEODE_170);
    final InternalDistributedMember mockInternalDistributedMember =
        createInternalDistributedMember();
    final TXId txId = new TXId(mockInternalDistributedMember, 0);
    final TXState txState = new TXState(null, false);
    final TXCommitMessage txCommitMessage = new TXCommitMessage(txId, null, txState);
    txCommitMessage.setClientVersion(Version.GEODE_170);
    final InternalRegion mockInternalRegion =
        createMockInternalRegion(mockInternalDistributedMember);
    txCommitMessage.startRegion(mockInternalRegion, 0);
    final EntryEventImpl mockEntryEventImpl = createMockEntryEvent(mockInternalRegion);
    final TXEntryState txEntryState = createTxEntryState(mockInternalRegion, mockEntryEventImpl);
    txCommitMessage.addOp(null, "key", txEntryState, null);
    txCommitMessage.finishRegionComplete();
    txCommitMessage.toData(mockDataOutput);

    // Asserts
    InOrder dataOutputInOrder = inOrder(mockDataOutput);
    // processor id
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeInt(anyInt());
    // txId.uniqId
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeInt(0);
    // lockId
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeBoolean(false);
    // totalMaxSize
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeInt(0);
    // txState.membershipId
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(-1);
    // txState.baseThreadId, txState.baseSequenceId
    dataOutputInOrder.verify(mockDataOutput, times(2)).writeLong(anyLong());
    // txState.needsLargeModCount
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeBoolean(false);
    // regionsSize
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeInt(1);
    // regionPath: "/r"
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(87);
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeShort(2);
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeBytes("/r");
    // parentRegionPath: null string
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(69);
    // opKeys size
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeInt(1);
    // needsLargeModCount
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeBoolean(false);
    // versionMember
    dataOutputInOrder.verify(mockDataOutput, times(2)).writeByte(1);
    // opKeys[0]
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(87);
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeShort(3);
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeBytes("key");
    // farSideData[0]
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(17);
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(0);
    dataOutputInOrder.verify(mockDataOutput, times(3)).writeByte(41);
    // no shadowkey offset
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeInt(anyInt());
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeBoolean(false);
    // bridgeContext
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(41);
    // farSiders
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(-1);
  }

  @Test
  public void toDataWithShadowKeyPost180Server() throws IOException {
    final VersionedDataOutput mockDataOutput = mock(VersionedDataOutput.class);
    when(mockDataOutput.getVersion()).thenReturn(Version.CURRENT);
    final InternalDistributedMember mockInternalDistributedMember =
        createInternalDistributedMember();
    final TXId txId = new TXId(mockInternalDistributedMember, 0);
    final TXState txState = new TXState(null, false);
    final TXCommitMessage txCommitMessage = new TXCommitMessage(txId, null, txState);
    final InternalRegion mockInternalRegion =
        createMockInternalRegion(mockInternalDistributedMember);
    txCommitMessage.startRegion(mockInternalRegion, 0);
    final EntryEventImpl mockEntryEventImpl = createMockEntryEvent(mockInternalRegion);
    final TXEntryState txEntryState = createTxEntryState(mockInternalRegion, mockEntryEventImpl);
    txCommitMessage.addOp(null, "key", txEntryState, null);
    txCommitMessage.finishRegionComplete();
    txCommitMessage.toData(mockDataOutput);

    // Asserts
    InOrder dataOutputInOrder = inOrder(mockDataOutput);
    // processor id
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeInt(anyInt());
    // txId.uniqId
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeInt(0);
    // lockId
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeBoolean(false);
    // totalMaxSize
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeInt(0);
    // txState.membershipId
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(-1);
    // txState.baseThreadId, txState.baseSequenceId
    dataOutputInOrder.verify(mockDataOutput, times(2)).writeLong(anyLong());
    // txState.needsLargeModCount
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeBoolean(false);
    // hasShadowKeys
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeBoolean(true);
    // regionsSize
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeInt(1);
    // regionPath: "/r"
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(87);
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeShort(2);
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeBytes("/r");
    // parentRegionPath: null string
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(69);
    // opKeys size
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeInt(1);
    // needsLargeModCount
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeBoolean(false);
    // versionMember
    dataOutputInOrder.verify(mockDataOutput, times(2)).writeByte(1);
    // opKeys[0]
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(87);
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeShort(3);
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeBytes("key");
    // farSideData[0]
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(17);
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(0);
    dataOutputInOrder.verify(mockDataOutput, times(3)).writeByte(41);
    // shadowkey
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeLong(-1L);
    // offset
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeInt(anyInt());
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeBoolean(false);
    // bridgeContext
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(41);
    // farSiders
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(-1);
  }

  @Test
  public void toDataWithoutShadowKeyPost180Client() throws IOException {
    final VersionedDataOutput mockDataOutput = mock(VersionedDataOutput.class);
    when(mockDataOutput.getVersion()).thenReturn(Version.CURRENT);
    final InternalDistributedMember mockInternalDistributedMember =
        createInternalDistributedMember();
    final TXId txId = new TXId(mockInternalDistributedMember, 0);
    final TXState txState = new TXState(null, false);
    final TXCommitMessage txCommitMessage = new TXCommitMessage(txId, null, txState);
    txCommitMessage.setClientVersion(Version.CURRENT);
    final InternalRegion mockInternalRegion =
        createMockInternalRegion(mockInternalDistributedMember);
    txCommitMessage.startRegion(mockInternalRegion, 0);
    final EntryEventImpl mockEntryEventImpl = createMockEntryEvent(mockInternalRegion);
    final TXEntryState txEntryState = createTxEntryState(mockInternalRegion, mockEntryEventImpl);
    txCommitMessage.addOp(null, "key", txEntryState, null);
    txCommitMessage.finishRegionComplete();
    txCommitMessage.toData(mockDataOutput);

    // Asserts
    InOrder dataOutputInOrder = inOrder(mockDataOutput);
    // processor id
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeInt(anyInt());
    // txId.uniqId
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeInt(0);
    // lockId
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeBoolean(false);
    // totalMaxSize
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeInt(0);
    // txState.membershipId
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(-1);
    // txState.baseThreadId, txState.baseSequenceId
    dataOutputInOrder.verify(mockDataOutput, times(2)).writeLong(anyLong());
    // txState.needsLargeModCount
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeBoolean(false);
    // hasShadowKeys
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeBoolean(false);
    // regionsSize
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeInt(1);
    // regionPath: "/r"
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(87);
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeShort(2);
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeBytes("/r");
    // parentRegionPath: null string
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(69);
    // opKeys size
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeInt(1);
    // needsLargeModCount
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeBoolean(false);
    // versionMember
    dataOutputInOrder.verify(mockDataOutput, times(2)).writeByte(1);
    // opKeys[0]
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(87);
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeShort(3);
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeBytes("key");
    // farSideData[0]
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(17);
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(0);
    dataOutputInOrder.verify(mockDataOutput, times(3)).writeByte(41);
    // no shadowkeyoffset
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeInt(anyInt());
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeBoolean(false);
    // bridgeContext
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(41);
    // farSiders
    dataOutputInOrder.verify(mockDataOutput, times(1)).writeByte(-1);
  }

  @Test
  public void fromDataWithShadowKeyPre180Server() throws Exception {
    final VersionedDataInput mockDataInput = mock(VersionedDataInput.class);
    when(mockDataInput.getVersion()).thenReturn(Version.GEODE_170);
    when(mockDataInput.readInt()).thenReturn(/* processor id */0, /* txId.uniqId */0,
        /* member version */0, 0, 0, 0, /* totalMaxSize */0, /* regionsSize */1,
        /* opKeys size */ 1, /* offset */0);
    when(mockDataInput.readByte()).thenReturn(/* member version */(byte) -1, (byte) 69, (byte) -1,
        (byte) 69, (byte) 69, /* durableId */(byte) 87, (byte) 0,
        /* txState.membershipId */(byte) -1, /* regionPath: "/r" */(byte) 87, (byte) 69,
        /* versionMember */(byte) 41, /* opKeys[0] */(byte) 87, /* farSideData[0] */(byte) 17,
        (byte) 0, (byte) 41, (byte) 41, (byte) 41, /* bridgeContext */(byte) 41,
        /* farSiders */(byte) -1);
    when(mockDataInput.readUnsignedByte()).thenReturn(/* member version */0, 1);
    when(mockDataInput.readUnsignedShort()).thenReturn(/* durableId */0, 2, 3);
    when(mockDataInput.readLong()).thenReturn(/* durableId */0L, 0L, /* txState.baseThreadId */0L,
        /* txState.baseSequenceId */0L, /* shadowkey */ -1L);
    when(mockDataInput.readBoolean()).thenReturn(/* lockId */false,
        /* txState.needsLargeModCount */false, /* needsLargeModCount */false, false);

    final DistributionManager mockDistributionManager = mock(DistributionManager.class);
    when(mockDistributionManager.getCache()).thenReturn(null);
    when(mockDistributionManager.isLoner()).thenReturn(false);
    final TXCommitMessage txCommitMessage = new TXCommitMessage();
    txCommitMessage.setDM(mockDistributionManager);
    verify(mockDataInput, never()).readInt();
    verify(mockDataInput, never()).readByte();
    verify(mockDataInput, never()).readUnsignedByte();
    verify(mockDataInput, never()).readUnsignedShort();
    verify(mockDataInput, never()).readLong();
    verify(mockDataInput, never()).readBoolean();
    txCommitMessage.fromData(mockDataInput);

    // Asserts
    verify(mockDataInput, times(10)).readInt();
    verify(mockDataInput, times(19)).readByte();
    verify(mockDataInput, times(2)).readUnsignedByte();
    verify(mockDataInput, times(3)).readUnsignedShort();
    verify(mockDataInput, times(5)).readLong();
    verify(mockDataInput, times(4)).readBoolean();
    verify(mockDataInput, times(3)).readFully(any(byte[].class), anyInt(), anyInt());
  }

  @Test
  public void fromDataWithShadowKeyPost180Server() throws Exception {
    final DataInput mockDataInput = mock(DataInput.class);
    when(mockDataInput.readInt()).thenReturn(/* processor id */0, /* txId.uniqId */0,
        /* member version */0, 0, 0, 0, /* totalMaxSize */0, /* regionsSize */1,
        /* opKeys size */ 1, /* offset */0);
    when(mockDataInput.readByte()).thenReturn(/* member version */(byte) -1, (byte) 69, (byte) -1,
        (byte) 69, (byte) 69, /* durableId */(byte) 87, (byte) 0,
        /* txState.membershipId */(byte) -1, /* regionPath: "/r" */(byte) 87, (byte) 69,
        /* versionMember */(byte) 41, /* opKeys[0] */(byte) 87, /* farSideData[0] */(byte) 17,
        (byte) 0, (byte) 41, (byte) 41, (byte) 41, /* bridgeContext */(byte) 41,
        /* farSiders */(byte) -1);
    when(mockDataInput.readUnsignedByte()).thenReturn(/* member version */0, 1);
    when(mockDataInput.readUnsignedShort()).thenReturn(/* durableId */0, 2, 3);
    when(mockDataInput.readLong()).thenReturn(/* durableId */0L, 0L, /* txState.baseThreadId */0L,
        /* txState.baseSequenceId */0L, /* shadowkey */ -1L);
    when(mockDataInput.readBoolean()).thenReturn(/* lockId */false,
        /* txState.needsLargeModCount */false, /* hasShadowKeys */true,
        /* needsLargeModCount */false, false);

    final DistributionManager mockDistributionManager = mock(DistributionManager.class);
    when(mockDistributionManager.getCache()).thenReturn(null);
    when(mockDistributionManager.isLoner()).thenReturn(false);
    final TXCommitMessage txCommitMessage = new TXCommitMessage();
    txCommitMessage.setDM(mockDistributionManager);
    txCommitMessage.fromData(mockDataInput);

    // Asserts
    verify(mockDataInput, times(10)).readInt();
    verify(mockDataInput, times(19)).readByte();
    verify(mockDataInput, times(2)).readUnsignedByte();
    verify(mockDataInput, times(3)).readUnsignedShort();
    verify(mockDataInput, times(5)).readLong();
    verify(mockDataInput, times(5)).readBoolean();
    verify(mockDataInput, times(3)).readFully(any(byte[].class), anyInt(), anyInt());
  }

  @Test
  public void fromDataWithoutShadowKeyPost180Client() throws Exception {
    final DataInput mockDataInput = mock(DataInput.class);
    when(mockDataInput.readInt()).thenReturn(/* processor id */0, /* txId.uniqId */0,
        /* member version */0, 0, 0, 0, /* totalMaxSize */0, /* regionsSize */1,
        /* opKeys size */ 1, /* offset */0);
    when(mockDataInput.readByte()).thenReturn(/* member version */(byte) -1, (byte) 69, (byte) -1,
        (byte) 69, (byte) 69, /* durableId */(byte) 87, (byte) 0,
        /* txState.membershipId */(byte) -1, /* regionPath: "/r" */(byte) 87, (byte) 69,
        /* versionMember */(byte) 41, /* opKeys[0] */(byte) 87, /* farSideData[0] */(byte) 17,
        (byte) 0, (byte) 41, (byte) 41, (byte) 41, /* bridgeContext */(byte) 41,
        /* farSiders */(byte) -1);
    when(mockDataInput.readUnsignedByte()).thenReturn(/* member version */0, 1);
    when(mockDataInput.readUnsignedShort()).thenReturn(/* durableId */0, 2, 3);
    when(mockDataInput.readLong()).thenReturn(/* durableId */0L, 0L, /* txState.baseThreadId */0L,
        /* txState.baseSequenceId */0L);
    when(mockDataInput.readBoolean()).thenReturn(/* lockId */false,
        /* txState.needsLargeModCount */false, /* hasShadowKeys */false,
        /* needsLargeModCount */false, false);
    final DistributionManager mockDistributionManager = mock(DistributionManager.class);
    when(mockDistributionManager.getCache()).thenReturn(null);
    when(mockDistributionManager.isLoner()).thenReturn(false);

    final TXCommitMessage txCommitMessage = new TXCommitMessage();
    txCommitMessage.setDM(mockDistributionManager);
    txCommitMessage.fromData(mockDataInput);

    // Asserts
    verify(mockDataInput, times(10)).readInt();
    verify(mockDataInput, times(19)).readByte();
    verify(mockDataInput, times(2)).readUnsignedByte();
    verify(mockDataInput, times(3)).readUnsignedShort();
    verify(mockDataInput, times(4)).readLong();
    verify(mockDataInput, times(5)).readBoolean();
    verify(mockDataInput, times(3)).readFully(any(byte[].class), anyInt(), anyInt());
  }
}
