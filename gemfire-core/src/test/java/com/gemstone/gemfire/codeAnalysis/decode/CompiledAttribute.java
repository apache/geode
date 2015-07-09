/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.codeAnalysis.decode;
import java.io.*;

import com.gemstone.gemfire.codeAnalysis.decode.cp.CpUtf8;


public class CompiledAttribute {
    int attribute_name_index;  // java predefines some but others are allowed, too
    long attribute_length;
    byte info[];
    String name;

    CompiledAttribute( DataInputStream source ) throws IOException {
        int idx;

        attribute_name_index = source.readUnsignedShort();
        attribute_length = source.readInt();
        info = new byte[(int)attribute_length];
        for (idx=0; idx<attribute_length; idx++) {
            info[idx] = (byte)source.readUnsignedByte();
        }
    }
    
    public String name(CompiledClass dclass) {
      if (name == null) {
        name = ((CpUtf8)dclass.constant_pool[attribute_name_index]).stringValue();
      }
      return name;
    }
}