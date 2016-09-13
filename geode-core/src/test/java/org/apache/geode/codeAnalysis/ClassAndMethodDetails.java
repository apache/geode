/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.codeAnalysis;

import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.codeAnalysis.decode.CompiledClass;
import com.gemstone.gemfire.codeAnalysis.decode.CompiledCode;
import com.gemstone.gemfire.codeAnalysis.decode.CompiledMethod;

/**
 * A class used to store the names of dataserializable classes and the sizes
 * of their toData/fromData methods.
 */
public class ClassAndMethodDetails implements Comparable {

  static String[] hexChars;

  public String className;
  public Map<String, byte[]> methodCode = new HashMap<String, byte[]>();
  
  static {
    String[] digits = new String[]{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"};
    
    hexChars = new String[256];
    for (int i=0; i<256; i++) {
      hexChars[i] = digits[(i>>4)&0xf] + digits[i&0xf];
    }
  }
  
  public ClassAndMethodDetails(CompiledClass dclass) {
    this.className = dclass.fullyQualifiedName();
  }
  
  private ClassAndMethodDetails() {
  }

  public static ClassAndMethodDetails create(LineNumberReader in) throws IOException {
    String line;
    while ((line = in.readLine()) != null) {
      line = line.trim();
      if (line.length() == 0 || line.startsWith("#") || line.startsWith("//")) {
        continue;
      }
      break;
    }
    if (line == null) {
      return null;
    }
    ClassAndMethodDetails instance = new ClassAndMethodDetails();
    String[] fields = line.split(",");
    try {
      instance.className = fields[0];
      int numMethods = Integer.parseInt(fields[1]);
      for (int i=0; i<numMethods; i++) {
        line = in.readLine();
        fields = line.split(",");
        String methodName = fields[0];
        int codeLength = Integer.parseInt(fields[1]);
        String codeString = fields[2].trim();
        int codeStringLength = codeString.length();
        if (codeStringLength != codeLength*2) {
          System.err.println("Code string has been tampered with on line " + in.getLineNumber());
          continue;
        }
        byte[] code = new byte[codeLength];
        int codeIdx=0;
        for (int j=0; j<codeStringLength; j+=2) {
          String substr = codeString.substring(j, j+2);
//            System.out.println("parsing " + j + ": '" + substr + "'");
          code[codeIdx++] = (byte)(0xff & Integer.parseInt(substr, 16));
        }
        instance.methodCode.put(methodName, code);
      }
      return instance;
    } catch (Exception e) {
      throw new IOException("Error parsing line " + in.getLineNumber(), e);
    }
  }
  
  /**
   * returns a string that can be parsed by ClassAndMethodDetails(String)
   */
  public String valuesAsString() {
    StringBuilder sb = new StringBuilder(80);
    sb.append(className).append(',').append(methodCode.size()).append("\n");
    for (Map.Entry<String,byte[]> entry: methodCode.entrySet()) {
      sb.append(entry.getKey()).append(',');
      byte[] code = entry.getValue();
      for (int i=0; i<code.length; i++) {
        sb.append(hexChars[(code[i] & 0xff)]);
      }
      sb.append("\n");
    }
    return sb.toString();
  }
  
  /**
   * convert a ClassAndMethods into a string that can then be used to
   * instantiate a ClassAndMethodDetails
   */
  public static String convertForStoring(ClassAndMethods cam) {
    StringBuilder sb = new StringBuilder(150);
    sb.append(cam.dclass.fullyQualifiedName());
    List<CompiledMethod> methods = new ArrayList<CompiledMethod>(cam.methods.values());
    Collections.sort(methods);
    sb.append(',').append(methods.size()).append("\n");
    for (CompiledMethod method: methods) {
      CompiledCode c = method.getCode();
      if (c != null) {
        sb.append(method.name())
          .append(',').append(c.code.length).append(',');
        for (int i=0; i<c.code.length; i++) {
          sb.append(hexChars[(c.code[i] & 0xff)]);
        }
        sb.append("\n");
      }
    }
    return sb.toString();
  }
  
  @Override
  public String toString() {
    return valuesAsString();
  }
  
  @Override
  public int compareTo(Object other) {
    return this.className.compareTo(((ClassAndMethodDetails)other).className);
  }
}
