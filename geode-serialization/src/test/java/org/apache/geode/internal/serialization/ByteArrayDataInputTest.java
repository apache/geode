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
package org.apache.geode.internal.serialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;

import org.junit.Test;

public class ByteArrayDataInputTest {
  @Test
  public void readFullyThatReadsPastEndOfDataThrowsEOFException() throws IOException {
    byte[] inputBytes = new byte[1];
    DataInput input = createDataInput(inputBytes);
    byte[] outputBytes = new byte[2];

    Throwable t = catchThrowable(() -> input.readFully(outputBytes));

    assertThat(t).isInstanceOf(EOFException.class);
  }

  @Test
  public void readLineGivenInputAtEOFReturnsNull() throws IOException {
    byte[] inputBytes = new byte[1];
    DataInput input = createDataInput(inputBytes);
    input.readByte();

    String result = input.readLine();

    assertThat(result).isNull();
    assertThat(dataRemaining(input)).isEqualTo(0);
  }

  @Test
  public void readLineGivenEmptyLineTerminatedByLineFeedReturnsEmptyString() throws IOException {
    byte[] inputBytes = new byte[] {'\n'};
    DataInput input = createDataInput(inputBytes);

    String result = input.readLine();

    assertThat(result).isEmpty();
    assertThat(dataRemaining(input)).isEqualTo(0);
  }

  @Test
  public void readLineGivenEmptyLineTerminatedByLineFeedReturnsEmptyStringDoesNotConsumeNextByte()
      throws IOException {
    byte[] inputBytes = new byte[] {'\n', 'a'};
    DataInput input = createDataInput(inputBytes);

    String result = input.readLine();

    assertThat(result).isEmpty();
    assertThat(dataRemaining(input)).isEqualTo(1);
    assertThat(input.readUnsignedByte()).isEqualTo('a');
  }

  @Test
  public void readLineGivenEmptyLineTerminatedByCarriageReturnReturnsEmptyString()
      throws IOException {
    byte[] inputBytes = new byte[] {'\r'};
    DataInput input = createDataInput(inputBytes);

    String result = input.readLine();

    assertThat(result).isEmpty();
    assertThat(dataRemaining(input)).isEqualTo(0);
  }

  @Test
  public void readLineGivenEmptyLineTerminatedByCarriageReturnReturnsEmptyStringAndDoesNotConsumeNextByte()
      throws IOException {
    byte[] inputBytes = new byte[] {'\r', 'a'};
    DataInput input = createDataInput(inputBytes);

    String result = input.readLine();

    assertThat(result).isEmpty();
    assertThat(dataRemaining(input)).isEqualTo(1);
    assertThat(input.readUnsignedByte()).isEqualTo('a');
  }

  @Test
  public void readLineGivenEmptyLineTerminatedByCarriageReturnLineFeedReturnsEmptyString()
      throws IOException {
    byte[] inputBytes = new byte[] {'\r', '\n'};
    DataInput input = createDataInput(inputBytes);

    String result = input.readLine();

    assertThat(result).isEmpty();
    assertThat(dataRemaining(input)).isEqualTo(0);
  }

  @Test
  public void readLineGivenEmptyLineTerminatedByCarriageReturnLineFeedReturnsEmptyStringDoesNotConsumeNextByte()
      throws IOException {
    byte[] inputBytes = new byte[] {'\r', '\n', 'a'};
    DataInput input = createDataInput(inputBytes);

    String result = input.readLine();

    assertThat(result).isEmpty();
    assertThat(dataRemaining(input)).isEqualTo(1);
    assertThat(input.readUnsignedByte()).isEqualTo('a');
  }

  @Test
  public void readLineGivenLineTerminatedByEOFReturnsCorrectLineData() throws IOException {
    byte[] inputBytes = new byte[] {'a', 'b', 'c'};
    DataInput input = createDataInput(inputBytes);

    String result = input.readLine();

    assertThat(result).isEqualTo("abc");
    assertThat(dataRemaining(input)).isEqualTo(0);
  }

  @Test
  public void readLineGivenLineTerminatedByLineFeedReturnsCorrectLineData() throws IOException {
    byte[] inputBytes = new byte[] {'a', 'b', 'c', '\n', '2'};
    DataInput input = createDataInput(inputBytes);

    String result = input.readLine();

    assertThat(result).isEqualTo("abc");
    assertThat(dataRemaining(input)).isEqualTo(1);
    assertThat(input.readUnsignedByte()).isEqualTo('2');
  }

  @Test
  public void readLineGivenLineTerminatedByCarriageReturnReturnsCorrectLineData()
      throws IOException {
    byte[] inputBytes = new byte[] {'a', 'b', 'c', '\r', '2'};
    DataInput input = createDataInput(inputBytes);

    String result = input.readLine();

    assertThat(result).isEqualTo("abc");
    assertThat(dataRemaining(input)).isEqualTo(1);
    assertThat(input.readUnsignedByte()).isEqualTo('2');
  }

  @Test
  public void readLineGivenLineTerminatedByCarriageReturnLineFeedReturnsCorrectLineData()
      throws IOException {
    byte[] inputBytes = new byte[] {'a', 'b', 'c', '\r', '\n', '2'};
    DataInput input = createDataInput(inputBytes);

    String result = input.readLine();

    assertThat(result).isEqualTo("abc");
    assertThat(dataRemaining(input)).isEqualTo(1);
    assertThat(input.readUnsignedByte()).isEqualTo('2');
  }

  @Test
  public void readUTFHandlesEmptyString() throws IOException {
    BufferDataOutputStream output = new BufferDataOutputStream(KnownVersion.CURRENT);
    output.writeUTF("");
    output.writeByte(1);
    DataInput input = createDataInput(output.toByteArray());

    String result = input.readUTF();

    assertThat(result).isEmpty();
    assertThat(dataRemaining(input)).isEqualTo(1);
    assertThat(input.readByte()).isEqualTo((byte) 1);
  }

  @Test
  public void readUTFHandlesAsciiString() throws IOException {
    BufferDataOutputStream output = new BufferDataOutputStream(KnownVersion.CURRENT);
    String string = "\u0001test\u007f";
    output.writeUTF(string);
    output.writeByte(1);
    DataInput input = createDataInput(output.toByteArray());

    String result = input.readUTF();

    assertThat(result).isEqualTo(string);
    assertThat(dataRemaining(input)).isEqualTo(1);
    assertThat(input.readByte()).isEqualTo((byte) 1);
  }

  @Test
  public void readUTFHandlesUTF16String() throws IOException {
    BufferDataOutputStream output = new BufferDataOutputStream(KnownVersion.CURRENT);
    String string = "\u0000test\u0080\uffff";
    output.writeUTF(string);
    output.writeByte(1);
    DataInput input = createDataInput(output.toByteArray());

    String result = input.readUTF();

    assertThat(result).isEqualTo(string);
    assertThat(dataRemaining(input)).isEqualTo(1);
    assertThat(input.readByte()).isEqualTo((byte) 1);
  }

  @Test
  public void readUTFOnInputWithJustLengthThrowsEOF() {
    BufferDataOutputStream output = new BufferDataOutputStream(KnownVersion.CURRENT);
    output.writeShort(1);
    DataInput input = createDataInput(output.toByteArray());

    Throwable t = catchThrowable(() -> input.readUTF());

    assertThat(t).isInstanceOf(EOFException.class);
  }

  /**
   * We want ByteArrayDataInput to behave like DataInputStream(ByteArrayInputStream).
   * This boolean allows us to switch back and forth in this test to make sure
   * they both behave the same. It should never be checked in with testJDK=true.
   */
  private final boolean testJDK = false;

  private DataInput createDataInput(byte[] inputBytes) {
    if (testJDK) {
      return new java.io.DataInputStream(new java.io.ByteArrayInputStream(inputBytes));
    } else {
      ByteArrayDataInput input = new ByteArrayDataInput();
      input.initialize(inputBytes, null);
      return input;
    }
  }

  private int dataRemaining(DataInput dataInput) {
    if (testJDK) {
      try {
        return ((java.io.DataInputStream) dataInput).available();
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    } else {
      return ((ByteArrayDataInput) dataInput).available();
    }
  }
}
