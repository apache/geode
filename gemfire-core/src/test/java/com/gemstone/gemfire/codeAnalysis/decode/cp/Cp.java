/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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