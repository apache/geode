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

import org.apache.geode.codeAnalysis.decode.CompiledClass;


public class CpFieldref extends Cp {
  int class_index;
  int name_and_type_index;

  CpFieldref(DataInputStream source) throws IOException {
    class_index = source.readUnsignedShort();
    name_and_type_index = source.readUnsignedShort();
  }

  public String className(CompiledClass info) {
    return ((CpClass) info.constant_pool[class_index]).className(info);
  }

  public String methodName(CompiledClass info) {
    return ((CpNameAndType) info.constant_pool[name_and_type_index]).name(info);
  }

  public String returnType(CompiledClass info) {
    return "not yet implemented";
  }
}
