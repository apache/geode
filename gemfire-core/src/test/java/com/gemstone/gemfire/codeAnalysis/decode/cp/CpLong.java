/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.codeAnalysis.decode.cp;
import java.io.*;

public class CpLong extends Cp {
    byte b1;
    byte b2;
    byte b3;
    byte b4;
    byte b5;
    byte b6;
    byte b7;
    byte b8;
    CpLong( DataInputStream source ) throws IOException {
        b1 = (byte)source.readUnsignedByte();
        b2 = (byte)source.readUnsignedByte();
        b3 = (byte)source.readUnsignedByte();
        b4 = (byte)source.readUnsignedByte();
        b5 = (byte)source.readUnsignedByte();
        b6 = (byte)source.readUnsignedByte();
        b7 = (byte)source.readUnsignedByte();
        b8 = (byte)source.readUnsignedByte();
    }
}