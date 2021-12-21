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
package org.apache.geode.internal.cache.versions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.VersionedDataInputStream;
import org.apache.geode.internal.tcp.ByteBufferInputStream;

public abstract class AbstractVersionTagTestBase {
  Set<Integer> usedInts = new HashSet<>();
  Random random = new Random();

  @SuppressWarnings("rawtypes")
  protected abstract VersionTag createVersionTag();

  @SuppressWarnings("rawtypes")
  protected abstract VersionSource createMemberID();

  @SuppressWarnings("rawtypes")
  private VersionTag vt;

  @Before
  public void setup() {
    vt = createVersionTag();
  }

  int getRandomUnusedInt() {
    int unusedInt;
    do {
      unusedInt = random.nextInt(60000);
    } while (usedInts.contains(unusedInt));
    usedInts.add(unusedInt);
    return unusedInt;
  }

  @Test
  public void testConcurrentCanonicalizationOfIDsAndSerialization() throws IOException {
    VersionTag spy = spy(vt);
    DataOutput dataOutput = mock(DataOutput.class);
    spy.setMemberID(createMemberID());
    spy.setPreviousMemberID(createMemberID());
    final short[] flags = {0};

    Answer myAnswer = new Answer() {
      boolean firstInvocation = true;

      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        if (firstInvocation) {
          // save the argument - it's the "flags" int that we'll want to verify
          flags[0] = (short) (((Integer) invocation.getArgument(0)).intValue() & 0xFFFF);
          firstInvocation = false;
          // canonicalize the member IDs. Once flags have been written the tag shouldn't examine
          // previousMemberID again to see if it's the same as the memberID.
          spy.setPreviousMemberID(spy.getMemberID());
        }
        return null;
      }
    };
    doAnswer(myAnswer).when(dataOutput).writeShort(any(Integer.class));
    spy.toData(dataOutput, true);
    // verify that we only wrote the
    verify(spy, times(2)).writeMember(isA(VersionSource.class), isA(DataOutput.class));
    assertThat(flags[0] & VersionTag.HAS_MEMBER_ID).isEqualTo(VersionTag.HAS_MEMBER_ID);
    assertThat(flags[0] & VersionTag.HAS_PREVIOUS_MEMBER_ID)
        .isEqualTo(VersionTag.HAS_PREVIOUS_MEMBER_ID);
    assertThat(flags[0] & VersionTag.DUPLICATE_MEMBER_IDS)
        .isNotEqualTo(VersionTag.DUPLICATE_MEMBER_IDS);
  }

  @Test
  public void testSerializationWritesNoMemberID() throws IOException {
    VersionTag spy = spy(vt);
    DataOutput dataOutput = mock(DataOutput.class);
    spy.setMemberID(createMemberID());
    spy.setPreviousMemberID(createMemberID());
    final short[] flags = {0};

    Answer myAnswer = new Answer() {
      boolean firstInvocation = true;

      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        if (firstInvocation) {
          // save the argument - it's the "flags" int that we'll want to verify
          flags[0] = (short) (((Integer) invocation.getArgument(0)).intValue() & 0xFFFF);
          firstInvocation = false;
        }
        return null;
      }
    };
    doAnswer(myAnswer).when(dataOutput).writeShort(any(Integer.class));
    spy.toData(dataOutput, false);
    // verify that we didn't write member IDs and the flags don't state that there are IDs in the
    // tag
    verify(spy, times(0)).writeMember(isA(VersionSource.class), isA(DataOutput.class));
    assertThat(flags[0] & VersionTag.HAS_MEMBER_ID).isNotEqualTo(VersionTag.HAS_MEMBER_ID);
    assertThat(flags[0] & VersionTag.HAS_PREVIOUS_MEMBER_ID)
        .isNotEqualTo(VersionTag.HAS_PREVIOUS_MEMBER_ID);
    assertThat(flags[0] & VersionTag.DUPLICATE_MEMBER_IDS)
        .isNotEqualTo(VersionTag.DUPLICATE_MEMBER_IDS);
  }

  @Test
  public void testBufferUnderflowFromOldVersionIsIgnored()
      throws IOException, ClassNotFoundException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream(1000);
    DataOutputStream out = new DataOutputStream(outputStream);
    short flags =
        VersionTag.HAS_MEMBER_ID | VersionTag.HAS_PREVIOUS_MEMBER_ID | VersionTag.VERSION_TWO_BYTES;
    out.writeShort(flags);
    out.writeShort(0);
    out.write(1);
    out.writeShort(12345);
    out.writeInt(12345);
    InternalDataSerializer.writeUnsignedVL(1L, out);
    VersionSource memberID = createMemberID();
    vt.writeMember(memberID, out);
    out.flush();

    ByteBufferInputStream inputStream =
        new ByteBufferInputStream(ByteBuffer.wrap(outputStream.toByteArray()));
    DataInputStream in = new DataInputStream(inputStream);
    VersionedDataInputStream versionedDataInputStream =
        new VersionedDataInputStream(in, KnownVersion.GEODE_1_10_0);
    DeserializationContext context =
        InternalDataSerializer.createDeserializationContext(versionedDataInputStream);

    // deserializing a version tag that's missing the "previous member id" should work for messages
    // from older nodes but not post-1.10 because the serialization problem was fixed
    vt = createVersionTag();
    vt.fromData(versionedDataInputStream, context);
    assertThat(vt.getMemberID()).isEqualTo(memberID);

    inputStream.position(0);
    final DataInputStream unversionedInputStream = new DataInputStream(inputStream);
    final DeserializationContext unversionedContext =
        InternalDataSerializer.createDeserializationContext(in);
    vt = createVersionTag();
    assertThatThrownBy(() -> vt.fromData(unversionedInputStream, unversionedContext))
        .isExactlyInstanceOf(BufferUnderflowException.class);
  }

  @Test
  public void testFromOtherMemberBit() {
    assertEquals(false, vt.isFromOtherMember());
    vt.setIsRemoteForTesting();
    assertEquals(true, vt.isFromOtherMember());
  }

  @Test
  public void testTimeStampUpdatedBit() {
    assertEquals(false, vt.isTimeStampUpdated());
    vt.setTimeStampApplied(true);
    assertEquals(true, vt.isTimeStampUpdated());
    vt.setTimeStampApplied(false);
    assertEquals(false, vt.isTimeStampUpdated());
  }

  @Test
  public void testGatewayTagBit() {
    assertEquals(false, vt.isGatewayTag());
    vt.setIsGatewayTag(true);
    assertEquals(true, vt.isGatewayTag());
    vt.setIsGatewayTag(false);
    assertEquals(false, vt.isGatewayTag());
  }

  @Test
  public void testRecordedBit() {
    assertEquals(false, vt.isRecorded());
    vt.setRecorded();
    assertEquals(true, vt.isRecorded());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testPreviousMemberIDBit() {
    assertEquals(false, vt.hasPreviousMemberID());
    vt.setPreviousMemberID(null);
    assertEquals(true, vt.hasPreviousMemberID());
  }

  @Test
  public void testPosDupBit() {
    assertEquals(false, vt.isPosDup());
    vt.setPosDup(true);
    assertEquals(true, vt.isPosDup());
    vt.setPosDup(false);
    assertEquals(false, vt.isPosDup());
  }

  @Test
  public void testAllowedByResolverBit() {
    assertEquals(false, vt.isAllowedByResolver());
    vt.setAllowedByResolver(true);
    assertEquals(true, vt.isAllowedByResolver());
    vt.setAllowedByResolver(false);
    assertEquals(false, vt.isAllowedByResolver());
  }
}
