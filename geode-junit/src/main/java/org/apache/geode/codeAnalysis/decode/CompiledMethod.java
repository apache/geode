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
package org.apache.geode.codeAnalysis.decode;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.geode.codeAnalysis.decode.cp.CpUtf8;


public class CompiledMethod implements Comparable {
  int access_flags;
  int name_index;
  int descriptor_index;
  int attributes_count;
  CompiledAttribute attributes[];
  String name;
  String descriptor;
  String accessString;
  String signature;
  String args;
  CompiledClass myclass;

  CompiledMethod(DataInputStream source, CompiledClass clazz) throws IOException {
    int idx;
    myclass = clazz; // may not be fully initialized at this point
    access_flags = source.readUnsignedShort();
    name_index = source.readUnsignedShort();
    descriptor_index = source.readUnsignedShort();
    attributes_count = source.readUnsignedShort();
    attributes = new CompiledAttribute[attributes_count];
    for (idx = 0; idx < attributes_count; idx++) {
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
    if ((access_flags & 0x0020) != 0)
      result.append("synchronized ");
    if ((access_flags & 0x0100) != 0)
      result.append("native ");
    if ((access_flags & 0x0400) != 0)
      result.append("abstract ");
    // if ((access_flags & 0x0800) != 0)
    // result.append("strict ");
    // if ((access_flags & 0x1000) != 0)
    // result.append("synthetic ");

    accessString = result.toString();
    return accessString;
  }

  public boolean isAbstract() {
    return (access_flags & 0x0400) != 0;
  }


  /**
   * the descriptor is just the type string for the return value
   */
  public String descriptor() {
    if (descriptor == null) {
      descriptor = ((CpUtf8) myclass.constant_pool[descriptor_index]).decodeClassName(0);
    }
    return descriptor;
  }

  public String name() {
    if (name == null) {
      name = ((CpUtf8) myclass.constant_pool[name_index]).stringValue();
    }
    return name;
  }

  public String args() {
    int argCount, idx;
    String str;

    if (args == null) {
      argCount = ((CpUtf8) myclass.constant_pool[descriptor_index]).argCount();
      for (idx = 1; idx <= argCount; idx++) {
        str = ((CpUtf8) myclass.constant_pool[descriptor_index]).decodeClassName(idx);
        if (args == null)
          args = str;
        else
          args = args + ", " + str;
      }
    }
    if (args == null)
      args = "";
    return args;
  }

  public boolean hasArgumentOfType(String compiledClassName) {
    int argCount, idx;

    if (args == null) {
      return false;
    }
    argCount = ((CpUtf8) myclass.constant_pool[descriptor_index]).argCount();
    for (idx = 1; idx <= argCount; idx++) {
      // use a cheap check here that doesn't require all of the work of decodeClassName()
      String str = ((CpUtf8) myclass.constant_pool[descriptor_index]).stringValue();
      if (compiledClassName.equals(str)) {
        return true;
      }
    }
    return false;
  }

  public String signature() {
    if (signature == null)
      signature = accessString() + descriptor() + " " + name() + "( " + args() + " );";
    return signature;
  }

  public CompiledCode getCode() {
    for (int i = 0; i < attributes.length; i++) {
      if (attributes[i].name(myclass).equals("Code")) {
        try {
          return new CompiledCode(attributes[i].info);
        } catch (IOException e) {
          e.printStackTrace(System.err);
        }
      }
    }
    return null;
  }

  @Override
  public int compareTo(Object other) {
    if (other == this) {
      return 0;
    }
    CompiledMethod otherMethod = (CompiledMethod) other;
    return signature().compareTo(otherMethod.signature());
  }

}
