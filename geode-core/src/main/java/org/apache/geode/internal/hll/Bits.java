/*
 * Copyright (C) 2011 Clearspring Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.hll;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

public class Bits {

    public static int[] getBits(byte[] mBytes) throws IOException {
        int bitSize = mBytes.length / 4;
        int[] bits = new int[bitSize];
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(mBytes));
        for (int i = 0; i < bitSize; i++) {
            bits[i] = dis.readInt();
        }
        return bits;
    }

    /**
     * This method might be better described as
     * "byte array to int array" or "data input to int array"
     */
    public static int[] getBits(DataInput dataIn, int byteLength) throws IOException {
        int bitSize = byteLength / 4;
        int[] bits = new int[bitSize];
        for (int i = 0; i < bitSize; i++) {
            bits[i] = dataIn.readInt();
        }
        return bits;
    }

}
