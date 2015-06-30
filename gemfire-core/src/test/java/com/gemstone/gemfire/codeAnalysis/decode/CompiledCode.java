/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.codeAnalysis.decode;

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
    
    CompiledCode( byte[] code_block ) throws IOException {
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
      for (int i=0; i<exception_table_length; i++) {
        exceptionTable[i] = new ExceptionTableEntry(source);
      }
      attributes_count = source.readUnsignedShort();
      attributes_info = new CompiledAttribute[attributes_count];
      for (idx=0; idx<attributes_count; idx++) {
          attributes_info[idx] = new CompiledAttribute(source);
      }
  }
    
  public static class ExceptionTableEntry {
    public int start_pc;
    public int end_pc;
    public int handler_pc;
    public int catch_type;
    
    ExceptionTableEntry( DataInputStream source ) throws IOException {
      start_pc = source.readUnsignedShort();
      end_pc = source.readUnsignedShort();
      handler_pc = source.readUnsignedShort();
      catch_type = source.readUnsignedShort();
    }
  }

}