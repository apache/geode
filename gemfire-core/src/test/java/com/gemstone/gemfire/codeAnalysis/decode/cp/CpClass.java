/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.codeAnalysis.decode.cp;
import java.io.*;

import com.gemstone.gemfire.codeAnalysis.decode.CompiledClass;


public class CpClass extends Cp {
    int name_index;
    private String name;

    CpClass( DataInputStream source ) throws IOException {
        name_index = source.readUnsignedShort();
    }
    /**
     * find and form the class name - remembering that '[' chars may come before
     * the class name to denote arrays of the given class.  Max of 255 array specs
     */
    public String className(CompiledClass info) {
        if (name == null)
            name = decodeNameRef(info);
        return name;
    }
    private String decodeNameRef(CompiledClass info) {
        return ((CpUtf8)info.constant_pool[name_index]).stringValue();
    }
}
