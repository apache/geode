/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.pdx.internal;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.DSCODE;
import com.gemstone.gemfire.internal.Sendable;
import com.gemstone.gemfire.internal.tcp.ByteBufferInputStream;

/**
 * A class that references the String offset in PdxInstance
 * Used as Index keys for PdxInstances and 
 * query evaluation for PdxInstances
 * @since 7.0
 */
public class PdxString implements Comparable<PdxString>, Sendable {
  private final byte[] bytes;
  private final int offset;
  private final byte header;
  //private int hash; // optimization: cache the hashcode

  public PdxString(byte[] bytes, int offset) {
    this.bytes = bytes;
    this.header = bytes[offset];
    this.offset = calcOffset(header,offset);
  }

  public PdxString(String s)  {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(s.length());
    try {
      DataSerializer.writeString(s, new DataOutputStream(bos));
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    this.bytes = bos.toByteArray();
    this.header = bytes[0];
    this.offset = calcOffset(header, 0);
  }

  private int calcOffset( int header, int offset) {
    offset++; // increment offset for the header byte
    // length is stored as short for small strings
    if (header == DSCODE.STRING_BYTES || header == DSCODE.STRING) {
      offset += 2; // position the offset to the start of the String
                        // (skipping header and length bytes)
    }
    // length is stored as int for huge strings
    else if (header == DSCODE.HUGE_STRING_BYTES || header == DSCODE.HUGE_STRING) {
      offset += 4; // position the offset to the start of the String
                        // (skipping header and length bytes)
    }
    return offset;
  }

  private int getLength() {
    int length = 0;
    int lenOffset = this.offset;
    if (header == DSCODE.STRING_BYTES || header == DSCODE.STRING) {
      lenOffset -= 2;
      byte a = bytes[lenOffset];
      byte b = bytes[lenOffset + 1];
      length = ((a & 0xff) << 8) | (b & 0xff);
    }
    // length is stored as int for huge strings
    else if (header == DSCODE.HUGE_STRING_BYTES || header == DSCODE.HUGE_STRING) {
      lenOffset -= 4;
      byte a = bytes[lenOffset];
      byte b = bytes[lenOffset + 1];
      byte c = bytes[lenOffset + 2];
      byte d = bytes[lenOffset + 3];
      length = (((a & 0xff) << 24) | ((b & 0xff) << 16) | ((c & 0xff) << 8) | (d & 0xff));
    }
    return length;
  }

  public int compareTo(PdxString o) {
    // not handling strings with different headers
    if(this.header != o.header){
      int diff =this.toString().compareTo(o.toString());
      return diff;
    }
    int len1 = this.getLength();
    int len2 = o.getLength();
    int n = Math.min(len1, len2);

    int i = this.offset;
    int j = o.offset;

    if (i == j) {
      int k = i;
      int lim = n + i;
      while (k < lim) {
        byte c1 = bytes[k];
        byte c2 = o.bytes[k];
        if (c1 != c2) {
          return c1 - c2;
        }
        k++;
      }
    } else {
      while (n-- != 0) {
        byte c1 = bytes[i++];
        byte c2 = o.bytes[j++];
        if (c1 != c2) {
          return c1 - c2;
        }
      }
    }
    return len1 - len2;
  }

  public int hashCode() {
    int h = 0;
    int len = this.getLength();
    if (len > 0) {
      int off = this.offset;
      for (int i = 0; i < len; i++) {
        h = 31 * h + bytes[off++];
      }
    }
    return h;
  }

  public boolean equals(Object anObject) {
     if (this == anObject) {
      return true;
    }
    if (anObject instanceof PdxString) {
      PdxString o = (PdxString) anObject;
      if(this.header != o.header){ //header needs to be same for Pdxstrings to be equal
        return false;
      }
      int n = this.getLength();
      if (n == o.getLength()) {
        int i = this.offset;
        int j = o.offset;
        while (n-- != 0) {
          if (bytes[i++] != o.bytes[j++])
            return false;
        }
        return true;
      }
    }
    return false;
  }

 
  public String toString() {
    String s = null;
    int headerOffset = this.offset;
    try {
      --headerOffset; // for header byte
      if (header == DSCODE.STRING_BYTES || header == DSCODE.STRING) {
        headerOffset -= 2; // position the offset to the start of the String (skipping
                     // header and length bytes)
      }
      // length is stored as int for huge strings
      else if (header == DSCODE.HUGE_STRING_BYTES
          || header == DSCODE.HUGE_STRING) {
        headerOffset -= 4;
      }
      ByteBuffer stringByteBuffer = ByteBuffer.wrap(bytes, headerOffset, bytes.length
          - headerOffset); // Wrapping more bytes than the actual String bytes in
                     // array. Counting on the readString() to read only String
                     // bytes
      s = DataSerializer.readString(new ByteBufferInputStream (stringByteBuffer));
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    return s;
  }

  @Override
  public void sendTo(DataOutput out) throws IOException {
    int offset = this.offset;
    int len = getLength();
    --offset;  // for header byte  
    len++;
    if (header == DSCODE.STRING_BYTES || header == DSCODE.STRING) {
      len+=2;
      offset -= 2; 
    }
    else if (header == DSCODE.HUGE_STRING_BYTES || header == DSCODE.HUGE_STRING) {
      len+=4;
      offset -= 4; 
    }
    out.write(bytes, offset, len);
  }

}
