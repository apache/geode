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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.Sequence;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.VersionedDataStream;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.test.junit.categories.UnitTest;

/**
 * Unit tests for TxCommitMessage.
 */
@Category(UnitTest.class)
public class TxCommitMessageTest {

  private Mockery mockContext;

  @Before
  public void setUp() {
    mockContext = new Mockery() {
      {
        setImposteriser(ClassImposteriser.INSTANCE);
        setThreadingPolicy(new Synchroniser());
      }
    };
  }

  @After
  public void tearDown() {
    mockContext.assertIsSatisfied();
    mockContext = null;
  }

  public interface VersionedDataOutput extends DataOutput, VersionedDataStream {

  }

  public interface VersionedDataInput extends DataInput, VersionedDataStream {

  }

  @Test
  public void toDataWithShadowKeyPre180Server() throws IOException {
    final Sequence toData = mockContext.sequence("toData");
    final VersionedDataOutput mockDataOutput = mockContext.mock(VersionedDataOutput.class);
    mockContext.checking(new Expectations() {
      {
        allowing(mockDataOutput).getVersion();
        will(returnValue(Version.GEODE_170));
        // processor id
        oneOf(mockDataOutput).writeInt(with(any(int.class)));
        inSequence(toData);
        // txId.uniqId
        oneOf(mockDataOutput).writeInt(0);
        inSequence(toData);
        // lockId
        oneOf(mockDataOutput).writeBoolean(false);
        inSequence(toData);
        // totalMaxSize
        oneOf(mockDataOutput).writeInt(0);
        inSequence(toData);
        // txState.membershipId
        oneOf(mockDataOutput).writeByte(-1);
        inSequence(toData);
        // txState.baseThreadId
        oneOf(mockDataOutput).writeLong(with(any(long.class)));
        inSequence(toData);
        // txState.baseSequenceId
        oneOf(mockDataOutput).writeLong(with(any(long.class)));
        inSequence(toData);
        // txState.needsLargeModCount
        oneOf(mockDataOutput).writeBoolean(false);
        inSequence(toData);
        // regionsSize
        oneOf(mockDataOutput).writeInt(1);
        inSequence(toData);

        // regionPath: "/r"
        oneOf(mockDataOutput).writeByte(87);
        inSequence(toData);
        oneOf(mockDataOutput).writeShort(2);
        inSequence(toData);
        oneOf(mockDataOutput).writeBytes("/r");
        inSequence(toData);
        // parentRegionPath: null string
        oneOf(mockDataOutput).writeByte(69);
        inSequence(toData);
        // opKeys size
        oneOf(mockDataOutput).writeInt(1);
        inSequence(toData);
        // needsLargeModCount
        oneOf(mockDataOutput).writeBoolean(false);
        inSequence(toData);
        // versionMember
        oneOf(mockDataOutput).writeByte(1);
        inSequence(toData);
        oneOf(mockDataOutput).writeByte(1);
        inSequence(toData);
        // opKeys[0]
        oneOf(mockDataOutput).writeByte(87);
        inSequence(toData);
        oneOf(mockDataOutput).writeShort(3);
        inSequence(toData);
        oneOf(mockDataOutput).writeBytes("key");
        inSequence(toData);
        // farSideData[0]
        oneOf(mockDataOutput).writeByte(17);
        inSequence(toData);
        oneOf(mockDataOutput).writeByte(0);
        inSequence(toData);
        oneOf(mockDataOutput).writeByte(41);
        inSequence(toData);
        oneOf(mockDataOutput).writeByte(41);
        inSequence(toData);
        oneOf(mockDataOutput).writeByte(41);
        inSequence(toData);
        // shadowkey
        oneOf(mockDataOutput).writeLong(-1L);
        inSequence(toData);
        // offset
        oneOf(mockDataOutput).writeInt(with(any(int.class)));
        inSequence(toData);
        oneOf(mockDataOutput).writeBoolean(false);
        inSequence(toData);

        // bridgeContext
        oneOf(mockDataOutput).writeByte(41);
        inSequence(toData);
        // farSiders
        oneOf(mockDataOutput).writeByte(-1);
        inSequence(toData);
      }
    });

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
  }

  @Test
  public void toDataWithoutShadowKeyPre180Client() throws IOException {
    final Sequence toData = mockContext.sequence("toData");
    final VersionedDataOutput mockDataOutput = mockContext.mock(VersionedDataOutput.class);
    mockContext.checking(new Expectations() {
      {
        allowing(mockDataOutput).getVersion();
        will(returnValue(Version.GEODE_170));
        // processor id
        oneOf(mockDataOutput).writeInt(with(any(int.class)));
        inSequence(toData);
        // txId.uniqId
        oneOf(mockDataOutput).writeInt(0);
        inSequence(toData);
        // lockId
        oneOf(mockDataOutput).writeBoolean(false);
        inSequence(toData);
        // totalMaxSize
        oneOf(mockDataOutput).writeInt(0);
        inSequence(toData);
        // txState.membershipId
        oneOf(mockDataOutput).writeByte(-1);
        inSequence(toData);
        // txState.baseThreadId
        oneOf(mockDataOutput).writeLong(with(any(long.class)));
        inSequence(toData);
        // txState.baseSequenceId
        oneOf(mockDataOutput).writeLong(with(any(long.class)));
        inSequence(toData);
        // txState.needsLargeModCount
        oneOf(mockDataOutput).writeBoolean(false);
        inSequence(toData);
        // regionsSize
        oneOf(mockDataOutput).writeInt(1);
        inSequence(toData);

        // regionPath: "/r"
        oneOf(mockDataOutput).writeByte(87);
        inSequence(toData);
        oneOf(mockDataOutput).writeShort(2);
        inSequence(toData);
        oneOf(mockDataOutput).writeBytes("/r");
        inSequence(toData);
        // parentRegionPath: null string
        oneOf(mockDataOutput).writeByte(69);
        inSequence(toData);
        // opKeys size
        oneOf(mockDataOutput).writeInt(1);
        inSequence(toData);
        // needsLargeModCount
        oneOf(mockDataOutput).writeBoolean(false);
        inSequence(toData);
        // versionMember
        oneOf(mockDataOutput).writeByte(1);
        inSequence(toData);
        oneOf(mockDataOutput).writeByte(1);
        inSequence(toData);
        // opKeys[0]
        oneOf(mockDataOutput).writeByte(87);
        inSequence(toData);
        oneOf(mockDataOutput).writeShort(3);
        inSequence(toData);
        oneOf(mockDataOutput).writeBytes("key");
        inSequence(toData);
        // farSideData[0]
        oneOf(mockDataOutput).writeByte(17);
        inSequence(toData);
        oneOf(mockDataOutput).writeByte(0);
        inSequence(toData);
        oneOf(mockDataOutput).writeByte(41);
        inSequence(toData);
        oneOf(mockDataOutput).writeByte(41);
        inSequence(toData);
        oneOf(mockDataOutput).writeByte(41);
        inSequence(toData);
        // no shadowkey
        // offset
        oneOf(mockDataOutput).writeInt(with(any(int.class)));
        inSequence(toData);
        oneOf(mockDataOutput).writeBoolean(false);
        inSequence(toData);

        // bridgeContext
        oneOf(mockDataOutput).writeByte(41);
        inSequence(toData);
        // farSiders
        oneOf(mockDataOutput).writeByte(-1);
        inSequence(toData);
      }
    });

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
  }

  @Test
  public void toDataWithShadowKeyPost180Server() throws IOException {
    final Sequence toData = mockContext.sequence("toData");
    final VersionedDataOutput mockDataOutput = mockContext.mock(VersionedDataOutput.class);
    mockContext.checking(new Expectations() {
      {
        allowing(mockDataOutput).getVersion();
        will(returnValue(Version.CURRENT));
        // processor id
        oneOf(mockDataOutput).writeInt(with(any(int.class)));
        inSequence(toData);
        // txId.uniqId
        oneOf(mockDataOutput).writeInt(0);
        inSequence(toData);
        // lockId
        oneOf(mockDataOutput).writeBoolean(false);
        inSequence(toData);
        // totalMaxSize
        oneOf(mockDataOutput).writeInt(0);
        inSequence(toData);
        // txState.membershipId
        oneOf(mockDataOutput).writeByte(-1);
        inSequence(toData);
        // txState.baseThreadId
        oneOf(mockDataOutput).writeLong(with(any(long.class)));
        inSequence(toData);
        // txState.baseSequenceId
        oneOf(mockDataOutput).writeLong(with(any(long.class)));
        inSequence(toData);
        // txState.needsLargeModCount
        oneOf(mockDataOutput).writeBoolean(false);
        inSequence(toData);
        // hasShadowKeys
        oneOf(mockDataOutput).writeBoolean(true);
        inSequence(toData);
        // regionsSize
        oneOf(mockDataOutput).writeInt(1);
        inSequence(toData);

        // regionPath: "/r"
        oneOf(mockDataOutput).writeByte(87);
        inSequence(toData);
        oneOf(mockDataOutput).writeShort(2);
        inSequence(toData);
        oneOf(mockDataOutput).writeBytes("/r");
        inSequence(toData);
        // parentRegionPath: null string
        oneOf(mockDataOutput).writeByte(69);
        inSequence(toData);
        // opKeys size
        oneOf(mockDataOutput).writeInt(1);
        inSequence(toData);
        // needsLargeModCount
        oneOf(mockDataOutput).writeBoolean(false);
        inSequence(toData);
        // versionMember
        oneOf(mockDataOutput).writeByte(1);
        inSequence(toData);
        oneOf(mockDataOutput).writeByte(1);
        inSequence(toData);
        // opKeys[0]
        oneOf(mockDataOutput).writeByte(87);
        inSequence(toData);
        oneOf(mockDataOutput).writeShort(3);
        inSequence(toData);
        oneOf(mockDataOutput).writeBytes("key");
        inSequence(toData);
        // farSideData[0]
        oneOf(mockDataOutput).writeByte(17);
        inSequence(toData);
        oneOf(mockDataOutput).writeByte(0);
        inSequence(toData);
        oneOf(mockDataOutput).writeByte(41);
        inSequence(toData);
        oneOf(mockDataOutput).writeByte(41);
        inSequence(toData);
        oneOf(mockDataOutput).writeByte(41);
        inSequence(toData);
        // shadowkey
        oneOf(mockDataOutput).writeLong(-1L);
        inSequence(toData);
        // offset
        oneOf(mockDataOutput).writeInt(with(any(int.class)));
        inSequence(toData);
        oneOf(mockDataOutput).writeBoolean(false);
        inSequence(toData);

        // bridgeContext
        oneOf(mockDataOutput).writeByte(41);
        inSequence(toData);
        // farSiders
        oneOf(mockDataOutput).writeByte(-1);
        inSequence(toData);
      }
    });

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
  }

  @Test
  public void toDataWithoutShadowKeyPost180Client() throws IOException {
    final Sequence toData = mockContext.sequence("toData");
    final VersionedDataOutput mockDataOutput = mockContext.mock(VersionedDataOutput.class);
    mockContext.checking(new Expectations() {
      {
        allowing(mockDataOutput).getVersion();
        will(returnValue(Version.CURRENT));
        // processor id
        oneOf(mockDataOutput).writeInt(with(any(int.class)));
        inSequence(toData);
        // txId.uniqId
        oneOf(mockDataOutput).writeInt(0);
        inSequence(toData);
        // lockId
        oneOf(mockDataOutput).writeBoolean(false);
        inSequence(toData);
        // totalMaxSize
        oneOf(mockDataOutput).writeInt(0);
        inSequence(toData);
        // txState.membershipId
        oneOf(mockDataOutput).writeByte(-1);
        inSequence(toData);
        // txState.baseThreadId
        oneOf(mockDataOutput).writeLong(with(any(long.class)));
        inSequence(toData);
        // txState.baseSequenceId
        oneOf(mockDataOutput).writeLong(with(any(long.class)));
        inSequence(toData);
        // txState.needsLargeModCount
        oneOf(mockDataOutput).writeBoolean(false);
        inSequence(toData);
        // hasShadowKeys
        oneOf(mockDataOutput).writeBoolean(false);
        inSequence(toData);
        // regionsSize
        oneOf(mockDataOutput).writeInt(1);
        inSequence(toData);

        // regionPath: "/r"
        oneOf(mockDataOutput).writeByte(87);
        inSequence(toData);
        oneOf(mockDataOutput).writeShort(2);
        inSequence(toData);
        oneOf(mockDataOutput).writeBytes("/r");
        inSequence(toData);
        // parentRegionPath: null string
        oneOf(mockDataOutput).writeByte(69);
        inSequence(toData);
        // opKeys size
        oneOf(mockDataOutput).writeInt(1);
        inSequence(toData);
        // needsLargeModCount
        oneOf(mockDataOutput).writeBoolean(false);
        inSequence(toData);
        // versionMember
        oneOf(mockDataOutput).writeByte(1);
        inSequence(toData);
        oneOf(mockDataOutput).writeByte(1);
        inSequence(toData);
        // opKeys[0]
        oneOf(mockDataOutput).writeByte(87);
        inSequence(toData);
        oneOf(mockDataOutput).writeShort(3);
        inSequence(toData);
        oneOf(mockDataOutput).writeBytes("key");
        inSequence(toData);
        // farSideData[0]
        oneOf(mockDataOutput).writeByte(17);
        inSequence(toData);
        oneOf(mockDataOutput).writeByte(0);
        inSequence(toData);
        oneOf(mockDataOutput).writeByte(41);
        inSequence(toData);
        oneOf(mockDataOutput).writeByte(41);
        inSequence(toData);
        oneOf(mockDataOutput).writeByte(41);
        inSequence(toData);
        // no shadowkey
        // offset
        oneOf(mockDataOutput).writeInt(with(any(int.class)));
        inSequence(toData);
        oneOf(mockDataOutput).writeBoolean(false);
        inSequence(toData);

        // bridgeContext
        oneOf(mockDataOutput).writeByte(41);
        inSequence(toData);
        // farSiders
        oneOf(mockDataOutput).writeByte(-1);
        inSequence(toData);
      }
    });

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
  }

  @Test
  public void fromDataWithShadowKeyPre180Server() throws Exception {
    final Sequence fromData = mockContext.sequence("fromData");
    final VersionedDataInput mockDataInput = mockContext.mock(VersionedDataInput.class);
    mockContext.checking(new Expectations() {
      {
        allowing(mockDataInput).getVersion();
        will(returnValue(Version.GEODE_170));
        // processor id
        oneOf(mockDataInput).readInt();
        will(returnValue(0));
        inSequence(fromData);
        // txId.uniqId
        oneOf(mockDataInput).readInt();
        will(returnValue(0));
        inSequence(fromData);
        // member version
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) -1));
        inSequence(fromData);
        oneOf(mockDataInput).readInt();
        will(returnValue(0));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 69));
        inSequence(fromData);
        oneOf(mockDataInput).readUnsignedByte();
        will(returnValue(0));
        inSequence(fromData);
        oneOf(mockDataInput).readInt();
        will(returnValue(0));
        inSequence(fromData);
        oneOf(mockDataInput).readInt();
        will(returnValue(0));
        inSequence(fromData);
        oneOf(mockDataInput).readUnsignedByte();
        will(returnValue(1));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) -1));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 69));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 69));
        inSequence(fromData);
        // durableId
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 87));
        inSequence(fromData);
        oneOf(mockDataInput).readUnsignedShort();
        will(returnValue(0));
        inSequence(fromData);
        oneOf(mockDataInput)
            .readFully(with(any(byte[].class)), with(any(int.class)), with(any(int.class)));
        inSequence(fromData);

        oneOf(mockDataInput).readInt();
        will(returnValue(0));
        inSequence(fromData);
        oneOf(mockDataInput).readLong();
        will(returnValue(0L));
        inSequence(fromData);
        oneOf(mockDataInput).readLong();
        will(returnValue(0L));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 0));
        inSequence(fromData);

        // lockId
        oneOf(mockDataInput).readBoolean();
        will(returnValue(false));
        inSequence(fromData);
        // totalMaxSize
        oneOf(mockDataInput).readInt();
        will(returnValue(0));
        inSequence(fromData);
        // txState.membershipId
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) -1));
        inSequence(fromData);
        // txState.baseThreadId
        oneOf(mockDataInput).readLong();
        will(returnValue(0L));
        inSequence(fromData);
        // txState.baseSequenceId
        oneOf(mockDataInput).readLong();
        will(returnValue(0L));
        inSequence(fromData);
        // txState.needsLargeModCount
        oneOf(mockDataInput).readBoolean();
        will(returnValue(false));
        inSequence(fromData);

        // regionsSize
        oneOf(mockDataInput).readInt();
        will(returnValue(1));
        inSequence(fromData);

        // regionPath: "/r"
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 87));
        inSequence(fromData);
        oneOf(mockDataInput).readUnsignedShort();
        will(returnValue(2));
        inSequence(fromData);
        oneOf(mockDataInput)
            .readFully(with(any(byte[].class)), with(any(int.class)), with(any(int.class)));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 69));
        inSequence(fromData);
        // opKeys size
        oneOf(mockDataInput).readInt();
        will(returnValue(1));
        inSequence(fromData);
        // needsLargeModCount
        oneOf(mockDataInput).readBoolean();
        will(returnValue(false));
        inSequence(fromData);
        // versionMember
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 41));
        inSequence(fromData);

        // opKeys[0]
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 87));
        inSequence(fromData);
        oneOf(mockDataInput).readUnsignedShort();
        will(returnValue(3));
        inSequence(fromData);
        oneOf(mockDataInput)
            .readFully(with(any(byte[].class)), with(any(int.class)), with(any(int.class)));
        inSequence(fromData);
        // farSideData[0]
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 17));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 0));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 41));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 41));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 41));
        inSequence(fromData);
        // shadowkey
        oneOf(mockDataInput).readLong();
        will(returnValue(-1L));
        inSequence(fromData);
        // offset
        oneOf(mockDataInput).readInt();
        will(returnValue(0));
        inSequence(fromData);
        oneOf(mockDataInput).readBoolean();
        will(returnValue(false));
        inSequence(fromData);

        // bridgeContext
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 41));
        inSequence(fromData);
        // farSiders
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) -1));
        inSequence(fromData);
      }
    });

    final DistributionManager mockDistributionManager =
        mockContext.mock(DistributionManager.class);
    mockContext.checking(new Expectations() {
      {
        allowing(mockDistributionManager).getCache();
        will(returnValue(null));
        allowing(mockDistributionManager).isLoner();
        will(returnValue(false));
      }
    });

    final TXCommitMessage txCommitMessage = new TXCommitMessage();
    txCommitMessage.setDM(mockDistributionManager);
    txCommitMessage.fromData(mockDataInput);
  }

  @Test
  public void fromDataWithShadowKeyPost180Server() throws Exception {
    final Sequence fromData = mockContext.sequence("fromData");
    final DataInput mockDataInput = mockContext.mock(DataInput.class);
    mockContext.checking(new Expectations() {
      {
        // processor id
        oneOf(mockDataInput).readInt();
        will(returnValue(0));
        inSequence(fromData);
        // txId.uniqId
        oneOf(mockDataInput).readInt();
        will(returnValue(0));
        inSequence(fromData);
        // member version
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) -1));
        inSequence(fromData);
        oneOf(mockDataInput).readInt();
        will(returnValue(0));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 69));
        inSequence(fromData);
        oneOf(mockDataInput).readUnsignedByte();
        will(returnValue(0));
        inSequence(fromData);
        oneOf(mockDataInput).readInt();
        will(returnValue(0));
        inSequence(fromData);
        oneOf(mockDataInput).readInt();
        will(returnValue(0));
        inSequence(fromData);
        oneOf(mockDataInput).readUnsignedByte();
        will(returnValue(1));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) -1));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 69));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 69));
        inSequence(fromData);
        // durableId
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 87));
        inSequence(fromData);
        oneOf(mockDataInput).readUnsignedShort();
        will(returnValue(0));
        inSequence(fromData);
        oneOf(mockDataInput)
            .readFully(with(any(byte[].class)), with(any(int.class)), with(any(int.class)));
        inSequence(fromData);

        oneOf(mockDataInput).readInt();
        will(returnValue(0));
        inSequence(fromData);
        oneOf(mockDataInput).readLong();
        will(returnValue(0L));
        inSequence(fromData);
        oneOf(mockDataInput).readLong();
        will(returnValue(0L));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 0));
        inSequence(fromData);

        // lockId
        oneOf(mockDataInput).readBoolean();
        will(returnValue(false));
        inSequence(fromData);
        // totalMaxSize
        oneOf(mockDataInput).readInt();
        will(returnValue(0));
        inSequence(fromData);
        // txState.membershipId
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) -1));
        inSequence(fromData);
        // txState.baseThreadId
        oneOf(mockDataInput).readLong();
        will(returnValue(0L));
        inSequence(fromData);
        // txState.baseSequenceId
        oneOf(mockDataInput).readLong();
        will(returnValue(0L));
        inSequence(fromData);
        // txState.needsLargeModCount
        oneOf(mockDataInput).readBoolean();
        will(returnValue(false));
        inSequence(fromData);

        // hasShadowKeys
        oneOf(mockDataInput).readBoolean();
        will(returnValue(true));
        inSequence(fromData);

        // regionsSize
        oneOf(mockDataInput).readInt();
        will(returnValue(1));
        inSequence(fromData);

        // regionPath: "/r"
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 87));
        inSequence(fromData);
        oneOf(mockDataInput).readUnsignedShort();
        will(returnValue(2));
        inSequence(fromData);
        oneOf(mockDataInput)
            .readFully(with(any(byte[].class)), with(any(int.class)), with(any(int.class)));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 69));
        inSequence(fromData);
        // opKeys size
        oneOf(mockDataInput).readInt();
        will(returnValue(1));
        inSequence(fromData);
        // needsLargeModCount
        oneOf(mockDataInput).readBoolean();
        will(returnValue(false));
        inSequence(fromData);
        // versionMember
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 41));
        inSequence(fromData);

        // opKeys[0]
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 87));
        inSequence(fromData);
        oneOf(mockDataInput).readUnsignedShort();
        will(returnValue(3));
        inSequence(fromData);
        oneOf(mockDataInput)
            .readFully(with(any(byte[].class)), with(any(int.class)), with(any(int.class)));
        inSequence(fromData);
        // farSideData[0]
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 17));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 0));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 41));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 41));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 41));
        inSequence(fromData);
        // shadowkey
        oneOf(mockDataInput).readLong();
        will(returnValue(-1L));
        inSequence(fromData);
        // offset
        oneOf(mockDataInput).readInt();
        will(returnValue(0));
        inSequence(fromData);
        oneOf(mockDataInput).readBoolean();
        will(returnValue(false));
        inSequence(fromData);

        // bridgeContext
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 41));
        inSequence(fromData);
        // farSiders
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) -1));
        inSequence(fromData);
      }
    });

    final DistributionManager mockDistributionManager =
        mockContext.mock(DistributionManager.class);
    mockContext.checking(new Expectations() {
      {
        allowing(mockDistributionManager).getCache();
        will(returnValue(null));
        allowing(mockDistributionManager).isLoner();
        will(returnValue(false));
      }
    });

    final TXCommitMessage txCommitMessage = new TXCommitMessage();
    txCommitMessage.setDM(mockDistributionManager);
    txCommitMessage.fromData(mockDataInput);
  }

  @Test
  public void fromDataWithoutShadowKeyPost180Client() throws Exception {
    final Sequence fromData = mockContext.sequence("fromData");
    final DataInput mockDataInput = mockContext.mock(DataInput.class);
    mockContext.checking(new Expectations() {
      {
        // processor id
        oneOf(mockDataInput).readInt();
        will(returnValue(0));
        inSequence(fromData);
        // txId.uniqId
        oneOf(mockDataInput).readInt();
        will(returnValue(0));
        inSequence(fromData);
        // member version
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) -1));
        inSequence(fromData);
        oneOf(mockDataInput).readInt();
        will(returnValue(0));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 69));
        inSequence(fromData);
        oneOf(mockDataInput).readUnsignedByte();
        will(returnValue(0));
        inSequence(fromData);
        oneOf(mockDataInput).readInt();
        will(returnValue(0));
        inSequence(fromData);
        oneOf(mockDataInput).readInt();
        will(returnValue(0));
        inSequence(fromData);
        oneOf(mockDataInput).readUnsignedByte();
        will(returnValue(1));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) -1));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 69));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 69));
        inSequence(fromData);
        // durableId
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 87));
        inSequence(fromData);
        oneOf(mockDataInput).readUnsignedShort();
        will(returnValue(0));
        inSequence(fromData);
        oneOf(mockDataInput)
            .readFully(with(any(byte[].class)), with(any(int.class)), with(any(int.class)));
        inSequence(fromData);

        oneOf(mockDataInput).readInt();
        will(returnValue(0));
        inSequence(fromData);
        oneOf(mockDataInput).readLong();
        will(returnValue(0L));
        inSequence(fromData);
        oneOf(mockDataInput).readLong();
        will(returnValue(0L));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 0));
        inSequence(fromData);

        // lockId
        oneOf(mockDataInput).readBoolean();
        will(returnValue(false));
        inSequence(fromData);
        // totalMaxSize
        oneOf(mockDataInput).readInt();
        will(returnValue(0));
        inSequence(fromData);
        // txState.membershipId
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) -1));
        inSequence(fromData);
        // txState.baseThreadId
        oneOf(mockDataInput).readLong();
        will(returnValue(0L));
        inSequence(fromData);
        // txState.baseSequenceId
        oneOf(mockDataInput).readLong();
        will(returnValue(0L));
        inSequence(fromData);
        // txState.needsLargeModCount
        oneOf(mockDataInput).readBoolean();
        will(returnValue(false));
        inSequence(fromData);

        // hasShadowKeys
        oneOf(mockDataInput).readBoolean();
        will(returnValue(false));
        inSequence(fromData);

        // regionsSize
        oneOf(mockDataInput).readInt();
        will(returnValue(1));
        inSequence(fromData);

        // regionPath: "/r"
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 87));
        inSequence(fromData);
        oneOf(mockDataInput).readUnsignedShort();
        will(returnValue(2));
        inSequence(fromData);
        oneOf(mockDataInput)
            .readFully(with(any(byte[].class)), with(any(int.class)), with(any(int.class)));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 69));
        inSequence(fromData);
        // opKeys size
        oneOf(mockDataInput).readInt();
        will(returnValue(1));
        inSequence(fromData);
        // needsLargeModCount
        oneOf(mockDataInput).readBoolean();
        will(returnValue(false));
        inSequence(fromData);
        // versionMember
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 41));
        inSequence(fromData);

        // opKeys[0]
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 87));
        inSequence(fromData);
        oneOf(mockDataInput).readUnsignedShort();
        will(returnValue(3));
        inSequence(fromData);
        oneOf(mockDataInput)
            .readFully(with(any(byte[].class)), with(any(int.class)), with(any(int.class)));
        inSequence(fromData);
        // farSideData[0]
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 17));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 0));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 41));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 41));
        inSequence(fromData);
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 41));
        inSequence(fromData);
        // no shadowkey
        // offset
        oneOf(mockDataInput).readInt();
        will(returnValue(0));
        inSequence(fromData);
        oneOf(mockDataInput).readBoolean();
        will(returnValue(false));
        inSequence(fromData);

        // bridgeContext
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) 41));
        inSequence(fromData);
        // farSiders
        oneOf(mockDataInput).readByte();
        will(returnValue((byte) -1));
        inSequence(fromData);
      }
    });

    final DistributionManager mockDistributionManager =
        mockContext.mock(DistributionManager.class);
    mockContext.checking(new Expectations() {
      {
        allowing(mockDistributionManager).getCache();
        will(returnValue(null));
        allowing(mockDistributionManager).isLoner();
        will(returnValue(true));
      }
    });

    final TXCommitMessage txCommitMessage = new TXCommitMessage();
    txCommitMessage.setDM(mockDistributionManager);
    txCommitMessage.fromData(mockDataInput);
  }

  private InternalDistributedMember createInternalDistributedMember() throws IOException {
    final InternalDistributedMember mockInternalDistributedMember =
        mockContext.mock(InternalDistributedMember.class);
    mockContext.checking(new Expectations() {
      {
        allowing(mockInternalDistributedMember).getDSFID();
        will(returnValue(1));
        allowing(mockInternalDistributedMember).toData(with(any(DataOutput.class)));
        allowing(mockInternalDistributedMember).getSerializationVersions();
        will(returnValue(null));
      }
    });
    return mockInternalDistributedMember;
  }

  private EntryEventImpl createMockEntryEvent(InternalRegion mockInternalRegion) {
    final EntryEventImpl mockEntryEventImpl = mockContext.mock(EntryEventImpl.class);
    mockContext.checking(new Expectations() {
      {
        allowing(mockEntryEventImpl).isLocalInvalid();
        will(returnValue(false));
        allowing(mockEntryEventImpl).getRegion();
        will(returnValue(mockInternalRegion));
        ignoring(mockEntryEventImpl).putValueTXEntry(with(any(TXEntryState.class)));
        ignoring(mockEntryEventImpl)
            .setTXEntryOldValue(with(any(Object.class)), with(any(boolean.class)));
      }
    });
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
    final InternalRegion mockInternalRegion = mockContext.mock(InternalRegion.class);
    mockContext.checking(new Expectations() {
      {
        allowing(mockInternalRegion).requiresReliabilityCheck();
        will(returnValue(false));
        allowing(mockInternalRegion).getVersionMember();
        will(returnValue(mockVersionSource));
        allowing(mockInternalRegion).getFullPath();
        will(returnValue("/r"));
        allowing(mockInternalRegion).getPersistBackup();
        will(returnValue(false));
        allowing(mockInternalRegion).getScope();
        will(returnValue(Scope.LOCAL));
        allowing(mockInternalRegion).isEntryEvictionPossible();
        will(returnValue(false));
        allowing(mockInternalRegion).isEntryExpiryPossible();
        will(returnValue(false));
        ignoring(mockInternalRegion).setInUseByTransaction(true);
        allowing(mockInternalRegion).getConcurrencyChecksEnabled();
        will(returnValue(false));
      }
    });
    return mockInternalRegion;
  }

}
