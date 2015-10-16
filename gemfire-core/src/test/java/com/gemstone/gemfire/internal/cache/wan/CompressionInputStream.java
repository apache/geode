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
package com.gemstone.gemfire.internal.cache.wan;

import java.io.*;

public class CompressionInputStream extends FilterInputStream
    implements CompressionConstants
{
    /*
     * Constructor calls constructor of superclass
     */
    public CompressionInputStream(InputStream in) {
        super(in);
    }
 
    /* 
     * Buffer of unpacked 6-bit codes 
     * from last 32 bits read.
     */
    int buf[] = new int[5];
 
    /*
     * Position of next code to read in buffer (5 signifies end). 
     */ 
    int bufPos = 5;
 
    /*
     * Reads in format code and decompresses character
     * accordingly.  
     */
    public int read() throws IOException {
        try {
            int code;

            // Read in and ignore empty bytes (NOP's) as 
            // long as they arrive. 
            do {
                code = readCode();
            } while (code == NOP);      
 
            if (code >= BASE) {
                // Retrieve index of character in codeTable 
                // if the code is in the correct range.

                return codeTable.charAt(code - BASE);
            } else if (code == RAW) {
                // read in the lower 4 bits and the 
                // higher 4 bits, and return the 
                // reconstructed character
                int high = readCode();
                int low = readCode();
                return (high << 4) | low;
            } else 
                throw new IOException("unknown compression code: " 
                                      + code);
        } catch (EOFException e) {
            // Return the end of file code
            return -1;
        }
    }
 
    /* 
     * This method reads up to len bytes from the input stream.
     * Returns if read blocks before len bytes are read. 
     */ 
    public int read(byte b[], int off, int len) 
        throws IOException 
    {
        if (len <= 0) {
            return 0;
        }
 
        // Read in a word and return -1 if no more data.
        int c = read();
        if (c == -1) {
            return -1;
        }

        // Save c in buffer b
        b[off] = (byte)c;
 
        int i = 1;
        // Try to read up to len bytes or until no
        // more bytes can be read without blocking.
        try {
            for (; (i < len) && (in.available() > 0); i++) {
                c = read();
                if (c == -1) {
                    break;
                }
                if (b != null) {
                    b[off + i] = (byte)c;
                }
            }
        } catch (IOException ee) {
        }
        return i;
    }

    /*
     * If there is no more data to decode left
     * in buf, read the next four bytes from the 
     * wire. Then store each group of 6 bits in an
     * element of buf.  Return one element of buf.
     */
    private int readCode() throws IOException {
        // As soon as all the data in buf has been read
        // (when bufPos == 5) read in another four bytes.
        if (bufPos == 5) {
            int b1 = in.read();
            int b2 = in.read();
            int b3 = in.read();
            int b4 = in.read();

            // make sure none of the bytes signify the
            // end of the data in the stream
            if ((b1 | b2 | b3 | b4) < 0)
                throw new EOFException();
            // Assign each group of 6 bits to an 
            // element of buf.
            int pack = (b1 << 24) | (b2 << 16) | 
                       (b3 << 8) | b4;
            buf[0] = (pack >>> 24) & 0x3F;
            buf[1] = (pack >>> 18) & 0x3F;
            buf[2] = (pack >>> 12) & 0x3F;
            buf[3] = (pack >>>  6) & 0x3F;
            buf[4] = (pack >>>  0) & 0x3F;
            bufPos = 0;
        }
        return buf[bufPos++];
    }
}