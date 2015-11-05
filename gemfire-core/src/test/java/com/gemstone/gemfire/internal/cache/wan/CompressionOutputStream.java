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

public class CompressionOutputStream extends FilterOutputStream
    implements CompressionConstants
{

    /*
     * Constructor calls constructor of superclass.
     */
    public CompressionOutputStream(OutputStream out) {
        super(out);
    }
 
    /* 
     * Buffer of 6-bit codes to pack into next 32-bit
     * word.  Five 6-bit codes fit into 4 words. 
     */
    int buf[] = new int[5];
 
    /*
     * Index of valid codes waiting in buf. 
     */
    int bufPos = 0;
 

    /*  
     * This method writes one byte to the socket stream. 
     */ 
    public void write(int b) throws IOException {
        // force argument to one byte
        b &= 0xFF;                      
 
        // Look up pos in codeTable to get its encoding. 
        int pos = codeTable.indexOf((char)b);

        if (pos != -1)
            // If pos is in the codeTable, write 
            // BASE + pos into buf. By adding BASE 
            // to pos, we know that the characters in
            // the codeTable will always have a code 
            // between 2 and 63 inclusive. This allows 
            // us to use RAW (RAW is equal to 1) to signify 
            // that the next two groups of 6-bits are necessary
            // for decompression of the next character. 

            writeCode(BASE + pos);

        else {
            // Otherwise, write RAW into buf to signify that
            // the Character is being sent in 12 bits.
            writeCode(RAW);

            // Write the last 4 bits of b into the buf.
            writeCode(b >> 4);

            // Truncate b to contain data in only the first 4
            // bits and write the first 4 bits of b into buf.
            writeCode(b & 0xF);
        }
    }
 
    /* 
     * This method writes up to len bytes to the socket stream. 
     */
    public void write(byte b[], int off, int len) 
        throws IOException 
    {
        // This implementation is quite inefficient because 
        // it has to call the other write method for every 
        // byte in the array.  It could be optimized for 
        // performance by doing all the processing in this 
        // method.

        for (int i = 0; i < len; i++)
            write(b[off + i]);
    }
 
   /* 
    * Clears buffer of all data (zeroes it out). 
    */ 
   public void flush() throws IOException {
        while (bufPos > 0)
            writeCode(NOP);
    }
 
    /* 
     * This method actually puts the data into the output stream
     * after packing the data from all 5 bytes in buf into one
     * word. Remember, each byte has, at most, 6 significant bits.
     */
    private void writeCode(int c) throws IOException {
        buf[bufPos++] = c;

        // write next word when we have 5 codes
        if (bufPos == 5) {      
            int pack = (buf[0] << 24) | (buf[1] << 18) | 
                       (buf[2] << 12) | (buf[3] << 6) | buf[4];
            out.write((pack >>> 24) & 0xFF);
            out.write((pack >>> 16) & 0xFF);
            out.write((pack >>> 8)  & 0xFF);
            out.write((pack >>> 0)  & 0xFF);
            bufPos = 0;
        }
    }
}