/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.codeAnalysis.decode.cp;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * Cp represents an entry in the constant pool of a class. See
 * https://docs.oracle.com/javase/specs/jvms/se8/html/jvms-4.html#jvms-4.4
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
  protected static final int TAG_MethodHandle = 15;
  protected static final int TAG_MethodType = 16;
  protected static final int TAG_InvokeDynamic = 18;

  public static Cp readCp(DataInputStream source) throws IOException {
    byte tag;

    tag = (byte) source.readUnsignedByte();
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
      case TAG_MethodHandle:
        return new CpMethodHandle(source);
      case TAG_MethodType:
        return new CpMethodType(source);
      case TAG_InvokeDynamic:
        return new CpInvokeDynamic(source);
      default:
        throw new IOException("Unknown tag type in constant pool: " + tag);
    }
  }
}
