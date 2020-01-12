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
package org.apache.geode.codeAnalysis;

import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.geode.codeAnalysis.decode.CompiledCode;
import org.apache.geode.codeAnalysis.decode.CompiledMethod;

/**
 * A class used to store the names of data-serializable classes and the sizes of their
 * toData/fromData methods.
 */
public class ClassAndMethodDetails implements Comparable {
  static String[] hexChars;
  String className;
  Map<String, Integer> methods = new HashMap<>();

  static {
    String[] digits = new String[] {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c",
        "d", "e", "f"};

    hexChars = new String[256];
    for (int i = 0; i < 256; i++) {
      hexChars[i] = digits[(i >> 4) & 0xf] + digits[i & 0xf];
    }
  }

  private ClassAndMethodDetails() {
    // Do nothing.
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
      for (int i = 0; i < numMethods; i++) {
        line = in.readLine();
        fields = line.split(",");
        String methodName = fields[0];
        int codeLength = Integer.parseInt(fields[1]);
        if (fields.length != 2) {
          System.err.println("Method detail has been tampered with on line " + in.getLineNumber());
          continue;
        }
        instance.methods.put(methodName, codeLength);
      }
      return instance;
    } catch (Exception e) {
      throw new IOException("Error parsing line " + in.getLineNumber(), e);
    }
  }

  /**
   * convert a ClassAndMethods into a string that can then be used to instantiate a
   * ClassAndMethodDetails
   */
  public static String convertForStoring(ClassAndMethods cam) {
    StringBuilder sb = new StringBuilder(150);
    sb.append(cam.dclass.fullyQualifiedName());
    List<CompiledMethod> methods = new ArrayList<CompiledMethod>(cam.methods.values());
    Collections.sort(methods);
    sb.append(',').append(methods.size()).append("\n");
    for (CompiledMethod method : methods) {
      CompiledCode c = method.getCode();
      if (c != null) {
        sb.append(method.name()).append(',').append(c.code.length).append("\n");
      }
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(80);
    sb.append(className).append(',').append(methods.size()).append("\n");
    for (Map.Entry<String, Integer> entry : methods.entrySet()) {
      sb.append(entry.getKey()).append(',');
      sb.append(entry.getValue());
      sb.append("\n");
    }
    return sb.toString();
  }

  @Override
  public int compareTo(Object other) {
    return this.className.compareTo(((ClassAndMethodDetails) other).className);
  }
}
