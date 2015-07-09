/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.codeAnalysis.decode.cp;
import java.io.*;
import java.util.*;

public class CpUtf8 extends Cp {
    private StringBuffer value;
    private String stringValue;
    private Vector classes;

    CpUtf8( DataInputStream source ) throws IOException {
        int len = source.readUnsignedShort();
        int idx;
        value = new StringBuffer();

        for (idx=0; idx<len; idx++) {
            value.append((char)source.readByte());
        }
    }
    public String stringValue() {
        if (stringValue == null)
            stringValue = value.toString();
        return stringValue;
    }

    /**
     * for a method descriptor, this will return the number of argument descriptions
     * found in the string
     */
    public int argCount() {
        if (classes == null)
            decodeClassName(0);
        return classes.size() - 1;
    }

    /**
     * decode the class name of the given argument, or the final class name
     * if the argument is zero
     */
    public String decodeClassName(int argNo) {
        int idx;

        if (classes == null) {
            classes = new Vector();
            idx = 0;
            while (idx >= 0)
                idx = decodeNextClassName(idx);
        }
        if (argNo == 0)
            return (String)classes.elementAt(classes.size()-1);
        else
            return (String)classes.elementAt(argNo-1);
    }

    private int decodeNextClassName(int startIdx) {
        int idx, len;
        StringBuffer str;
        StringBuffer arraySpec = new StringBuffer();

        idx = startIdx;
        len = value.length();
        if (idx >= len)
            return -1;

        while (value.charAt(idx) == ')' || value.charAt(idx) == '(')
            idx++;

        arraySpec = new StringBuffer();

        while (value.charAt(idx) == '[') {
            idx++;
            arraySpec.append("[]");
        }

        switch(value.charAt(idx)) {
            case 'B':
                str = new StringBuffer("byte");
                break;
            case 'C':
                str = new StringBuffer("char");
                break;
            case 'D':
                str = new StringBuffer("double");
                break;
            case 'F':
                str = new StringBuffer("float");
                break;
            case 'I':
                str = new StringBuffer("int");
                break;
            case 'J':
                str = new StringBuffer("long");
                break;
            case 'L':
                idx += 1;
                str = new StringBuffer();
                while (value.charAt(idx) != ';') {
                    str.append(value.charAt(idx));
                    idx += 1;
                }
                break;
            case 'S':
                str = new StringBuffer("short");
                break;
            case 'Z':
                str = new StringBuffer("boolean");
                break;
            case 'V':
                str = new StringBuffer("void");
                break;
            default:
                throw new Error("Unknown type specifier in descriptor: " + value.charAt(idx));
        }
        str.append(arraySpec);
        classes.addElement(str.toString());
        return idx + 1;
    }
}