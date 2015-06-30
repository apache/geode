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


public class CompiledField implements Comparable {
    int access_flags;
    int name_index;
    int descriptor_index;
    int attributes_count;
    // only the ConstantValue attribute is defined for fields
    CompiledAttribute attributes[];
    String name;
    String descriptor;
    String accessString;
    String signature;
    CompiledClass myclass;

    CompiledField( DataInputStream source, CompiledClass myclass ) throws IOException {
        int idx;

        this.myclass = myclass; // may not be fully initialized at this point
        access_flags = source.readUnsignedShort();
        name_index = source.readUnsignedShort();
        descriptor_index = source.readUnsignedShort();
        attributes_count = source.readUnsignedShort();
        attributes = new CompiledAttribute[attributes_count];
        for (idx=0; idx<attributes_count; idx++) {
            attributes[idx] = new CompiledAttribute(source);
        }
    }
    
    /**
     * return a string describing the access modifiers for this class
     */
    public String accessString() {
        StringBuffer result;

        if (accessString != null)
            return accessString;
        result = new StringBuffer();
        if ((access_flags & 0x0001) != 0)
            result.append("public ");
        if ((access_flags & 0x0002) != 0)
            result.append("private ");
        if ((access_flags & 0x0004) != 0)
            result.append("protected ");
        if ((access_flags & 0x0008) != 0)
            result.append("static ");
        if ((access_flags & 0x0010) != 0)
            result.append("final ");
        if ((access_flags & 0x0040) != 0)
            result.append("volatile ");
        if ((access_flags & 0x0080) != 0)
            result.append("transient ");

        accessString = result.toString();
        return accessString;
    }
    
    public boolean isStatic() {
      return (access_flags & 0x8) != 0;
    }

    public boolean isTransient() {
      return (access_flags & 0x80) != 0;
    }

    /**
     * the descriptor is just the type string for the field
     */
    public String descriptor() {
        if (descriptor == null) {
            descriptor = ((CpUtf8)myclass.constant_pool[descriptor_index]).decodeClassName(0);
        }
        return descriptor;
    }

    public String name() {
        if (name == null) {
            name = ((CpUtf8)myclass.constant_pool[name_index]).stringValue();
        }
        return name;
    }

    public String signature() {
        if (signature == null)
            signature = accessString() + descriptor() + " " + name() + ";";
        return signature;
    }
    
    public int compareTo(Object other) {
      if (this == other) {
        return 0;
      }
      CompiledField otherField = (CompiledField)(other);
      return name().compareTo(otherField.name);
    }
}
