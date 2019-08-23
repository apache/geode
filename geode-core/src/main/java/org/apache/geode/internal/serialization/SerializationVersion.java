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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class SerializationVersion implements Comparable<SerializationVersion> {

  /**
   * Reserved token that cannot be used for product version but as a flag in internal contexts.
   */
  protected static final byte TOKEN_ORDINAL = -1;
  protected static final int TOKEN_ORDINAL_INT = (TOKEN_ORDINAL & 0xFF);

  /** value used as ordinal to represent this <code>SerializationVersion</code> */
  protected final short ordinal;

  public SerializationVersion(int ordinal) {
    this.ordinal = (short) ordinal;
  }

  /**
   * Write the given ordinal (result of ordinal()) to given {@link DataOutput}. This keeps
   * the serialization of ordinal compatible with previous versions writing a single byte to
   * DataOutput when possible, and a token with 2 bytes if it is large.
   *
   * @param out the {@link DataOutput} to write the ordinal write to
   * @param ordinal the version to be written
   * @param compressed if true, then use single byte for ordinal < 128, and three bytes for beyond
   *        that, else always use three bytes where the first byte is {@link #TOKEN_ORDINAL}; former
   *        mode is useful for interoperatibility with previous versions while latter to use fixed
   *        size for writing version; typically former will be used for P2P/client-server
   *        communications while latter for persisting to disk; we use the token to ensure that
   *        {@link #readOrdinal(DataInput)} can deal with both compressed/uncompressed cases
   *        seemlessly
   */
  public static void writeOrdinal(DataOutput out, short ordinal, boolean compressed)
      throws IOException {
    if (compressed && ordinal <= Byte.MAX_VALUE) {
      out.writeByte(ordinal);
    } else {
      out.writeByte(TOKEN_ORDINAL);
      out.writeShort(ordinal);
    }
  }

  /**
   * Write the given ordinal (result of ordinal()) to given {@link ByteBuffer}. This keeps
   * the serialization of ordinal compatible with previous versions writing a single byte to
   * DataOutput when possible, and a token with 2 bytes if it is large.
   *
   * @param buffer the {@link ByteBuffer} to write the ordinal write to
   * @param ordinal the version to be written
   * @param compressed if true, then use single byte for ordinal < 128, and three bytes for beyond
   *        that, else always use three bytes where the first byte is {@link #TOKEN_ORDINAL}
   */
  public static void writeOrdinal(ByteBuffer buffer, short ordinal, boolean compressed) {
    if (compressed && ordinal <= Byte.MAX_VALUE) {
      buffer.put((byte) ordinal);
    } else {
      buffer.put(TOKEN_ORDINAL);
      buffer.putShort(ordinal);
    }
  }

  /**
   * Reads ordinal as written by {@link #writeOrdinal} from given {@link DataInput}.
   */
  public static short readOrdinal(DataInput in) throws IOException {
    final byte ordinal = in.readByte();
    if (ordinal != TOKEN_ORDINAL) {
      return ordinal;
    } else {
      return in.readShort();
    }
  }

  /**
   * Reads ordinal as written by writeOrdinal from given InputStream. Returns -1 on
   * end of stream.
   */
  public static short readOrdinalFromInputStream(InputStream is) throws IOException {
    final int ordinal = is.read();
    if (ordinal != -1) {
      if (ordinal != TOKEN_ORDINAL_INT) {
        return (short) ordinal;
      } else {
        // two byte ordinal
        final int ordinalPart1 = is.read();
        final int ordinalPart2 = is.read();
        if ((ordinalPart1 | ordinalPart2) >= 0) {
          return (short) ((ordinalPart1 << 8) | ordinalPart2);
        } else {
          return -1;
        }
      }
    } else {
      return -1;
    }
  }

  public String getMethodSuffix() {
    return "" + ordinal;
  }

  @Override
  public String toString() {
    return "Version{" +
        "ordinal=" + ordinal +
        '}';
  }

  @Override
  public int compareTo(SerializationVersion other) {
    if (other != null) {
      // byte min/max can't overflow int, so use (a-b)
      final int thisOrdinal = this.ordinal;
      final int otherOrdinal = other.ordinal;
      return (thisOrdinal - otherOrdinal);
    } else {
      return 1;
    }
  }
}
