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
package com.gemstone.gemfire.codeAnalysis.decode.cp;
import java.io.*;

/**
 * Cp represents an entry in the constant pool of a class
 */
public class Cp {

    protected static final int TAG_Class = 7;
    protected static final int TAG_Fieldref = 9;
    protected static final int TAG_Methodref = 10;
    protected static final int TAG_InterfaceMethodref = 11;
    protected static final int TAG_String = 8;
    protected static final int TAG_Integer = 3;
    protected static final int TAG_Float = 4;
    protected static final int TAG_Long = 5;
    protected static final int TAG_Double = 6;
    protected static final int TAG_NameAndType = 12;
    protected static final int TAG_Utf8 = 1;

    public static Cp readCp( DataInputStream source ) throws IOException {
        byte tag;

        tag = (byte)source.readUnsignedByte();
        switch (tag) {
            case TAG_Class:
                return new CpClass(source);
            case TAG_Fieldref:
                return new CpFieldref(source);
            case TAG_Methodref:
                return new CpMethodref(source);
            case TAG_InterfaceMethodref:
                return new CpInterfaceMethodref(source);
            case TAG_String:
                return new CpString(source);
            case TAG_Integer:
                return new CpInteger(source);
            case TAG_Float:
                return new CpFloat(source);
            case TAG_Long:
                return new CpLong(source);
            case TAG_Double:
                return new CpDouble(source);
            case TAG_NameAndType:
                return new CpNameAndType(source);
            case TAG_Utf8:
                return new CpUtf8(source);
            default:
                throw new Error("Unknown tag type in constant pool: " + tag);
        }
    }
}