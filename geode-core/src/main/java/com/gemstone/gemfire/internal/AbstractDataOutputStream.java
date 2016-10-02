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

package com.gemstone.gemfire.internal;

import com.gemstone.gemfire.DataSerializer;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Attempt to create abstract outputstream class for duplicate code
 * Created by zhouwei on 2016/10/2.
 */
public abstract class AbstractDataOutputStream extends OutputStream implements ObjToByteArraySerializer {
    /**
     * Writes a string to the output stream.
     * For every character in the string
     * <code>s</code>,  taken in order, one byte
     * is written to the output stream.  If
     * <code>s</code> is <code>null</code>, a <code>NullPointerException</code>
     * is thrown.<p>  If <code>s.length</code>
     * is zero, then no bytes are written. Otherwise,
     * the character <code>s[0]</code> is written
     * first, then <code>s[1]</code>, and so on;
     * the last character written is <code>s[s.length-1]</code>.
     * For each character, one byte is written,
     * the low-order byte, in exactly the manner
     * of the <code>writeByte</code> method . The
     * high-order eight bits of each character
     * in the string are ignored.
     *
     * @param      str  the string of bytes to be written.
     */
    public final void writeString(String str,ByteBuffer buffer){
        int strlen = str.length();
        if(strlen == 0)
            return;

        if (buffer.hasArray()) {
            // I know this is a deprecated method but it is PERFECT for this impl.
            int pos = buffer.position();
            str.getBytes(0, strlen, buffer.array(), buffer.arrayOffset() + pos);
            buffer.position(pos + strlen);
        } else {
            byte[] bytes = new byte[strlen];
            str.getBytes(0, strlen, bytes, 0);
            buffer.put(bytes);
        }
    }

    /**
     * Writes two bytes of length information
     * to the output stream, followed
     * by the Java modified UTF representation
     * of  every character in the string <code>s</code>.
     * If <code>s</code> is <code>null</code>,
     * a <code>NullPointerException</code> is thrown.
     * Each character in the string <code>s</code>
     * is converted to a group of one, two, or
     * three bytes, depending on the value of the
     * character.<p>
     * If a character <code>c</code>
     * is in the range <code>&#92;u0001</code> through
     * <code>&#92;u007f</code>, it is represented
     * by one byte:<p>
     * <pre>(byte)c </pre>  <p>
     * If a character <code>c</code> is <code>&#92;u0000</code>
     * or is in the range <code>&#92;u0080</code>
     * through <code>&#92;u07ff</code>, then it is
     * represented by two bytes, to be written
     * in the order shown:<p> <pre><code>
     * (byte)(0xc0 | (0x1f &amp; (c &gt;&gt; 6)))
     * (byte)(0x80 | (0x3f &amp; c))
     *  </code></pre>  <p> If a character
     * <code>c</code> is in the range <code>&#92;u0800</code>
     * through <code>uffff</code>, then it is
     * represented by three bytes, to be written
     * in the order shown:<p> <pre><code>
     * (byte)(0xe0 | (0x0f &amp; (c &gt;&gt; 12)))
     * (byte)(0x80 | (0x3f &amp; (c &gt;&gt;  6)))
     * (byte)(0x80 | (0x3f &amp; c))
     *  </code></pre>  <p> First,
     * the total number of bytes needed to represent
     * all the characters of <code>s</code> is
     * calculated. If this number is larger than
     * <code>65535</code>, then a <code>UTFDataFormatException</code>
     * is thrown. Otherwise, this length is written
     * to the output stream in exactly the manner
     * of the <code>writeShort</code> method;
     * after this, the one-, two-, or three-byte
     * representation of each character in the
     * string <code>s</code> is written.<p>  The
     * bytes written by this method may be read
     * by the <code>readUTF</code> method of interface
     * <code>DataInput</code> , which will then
     * return a <code>String</code> equal to <code>s</code>.
     *
     * @param      str   the string value to be written.
     * @param      buffer
     */
    protected void fillFullUTF(String str, ByteBuffer buffer){
        int strlen = str.length();
        if(strlen == 0)
            return;

        for (int i = 0; i < strlen; i++) {
            int c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                buffer.put((byte)c);
            } else if (c > 0x07FF) {
                buffer.put((byte) (0xE0 | ((c >> 12) & 0x0F)));
                buffer.put((byte) (0x80 | ((c >>  6) & 0x3F)));
                buffer.put((byte) (0x80 | ((c) & 0x3F)));
            } else {
                buffer.put((byte) (0xC0 | ((c >>  6) & 0x1F)));
                buffer.put((byte) (0x80 | ((c) & 0x3F)));
            }
        }
    }

    /**
     * Writes the given object to this stream as a byte array.
     * The byte array is produced by serializing v. The serialization
     * is done by calling DataSerializer.writeObject.
     */
    protected void writeSerializedByteArray(ByteBuffer buffer,Object v) throws IOException {
        ByteBuffer sizeBuf = buffer;
        int sizePos = sizeBuf.position();
        sizeBuf.position(sizePos + 5);
        final int preArraySize = size();
        DataSerializer.writeObject(v, this);
        int arraySize = size() - preArraySize;
        sizeBuf.put(sizePos, InternalDataSerializer.INT_ARRAY_LEN);
        sizeBuf.putInt(sizePos + 1, arraySize);
    }

    protected abstract int size();
}
