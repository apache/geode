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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class CompiledCode {
  public int max_stack;
  public int max_locals;
  public int code_length;
  public byte code[];
  public int exception_table_length;
  ExceptionTableEntry exceptionTable[];
  public int attributes_count;
  public CompiledAttribute attributes_info[];

  CompiledCode(byte[] code_block) throws IOException {
    int idx;

    ByteArrayInputStream bis = new ByteArrayInputStream(code_block);
    DataInputStream source = new DataInputStream(bis);

    max_stack = source.readUnsignedShort();
    max_locals = source.readUnsignedShort();
    code_length = source.readInt();
    code = new byte[code_length];
    source.read(code);
    exception_table_length = source.readUnsignedShort();
    exceptionTable = new ExceptionTableEntry[exception_table_length];
    for (int i = 0; i < exception_table_length; i++) {
      exceptionTable[i] = new ExceptionTableEntry(source);
    }
    attributes_count = source.readUnsignedShort();
    attributes_info = new CompiledAttribute[attributes_count];
    for (idx = 0; idx < attributes_count; idx++) {
      attributes_info[idx] = new CompiledAttribute(source);
    }
  }

  public static class ExceptionTableEntry {
    public int start_pc;
    public int end_pc;
    public int handler_pc;
    public int catch_type;

    ExceptionTableEntry(DataInputStream source) throws IOException {
      start_pc = source.readUnsignedShort();
      end_pc = source.readUnsignedShort();
      handler_pc = source.readUnsignedShort();
      catch_type = source.readUnsignedShort();
    }
  }

}
