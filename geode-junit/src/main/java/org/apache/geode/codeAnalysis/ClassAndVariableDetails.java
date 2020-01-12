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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.geode.codeAnalysis.decode.CompiledClass;
import org.apache.geode.codeAnalysis.decode.CompiledField;


/**
 * A class used to store the names of dataserializable classes and the sizes of their
 * toData/fromData methods.
 *
 *
 */
public class ClassAndVariableDetails implements Comparable {
  public String className;
  public boolean hasSerialVersionUID;
  public String serialVersionUID;
  public Map<String, String> variables = new HashMap<String, String>();

  public ClassAndVariableDetails(CompiledClass dclass) {
    this.className = dclass.fullyQualifiedName();
  }

  public ClassAndVariableDetails(String storedValues) throws IOException {
    String[] fields = storedValues.split(",");
    try {
      int fieldIndex = 2;
      className = fields[0];
      hasSerialVersionUID = Boolean.valueOf(fields[1]);
      if (hasSerialVersionUID) {
        serialVersionUID = fields[2];
        fieldIndex++;
      }
      for (int i = fieldIndex; i < fields.length; i++) {
        String[] nameAndType = fields[i].split(":");
        variables.put(nameAndType[0], nameAndType[1]);
      }
    } catch (Exception e) {
      throw new IOException("Error parsing " + storedValues, e);
    }
  }

  /**
   * returns a string that can be parsed by ClassAndVariableDetails(String)
   */
  public String valuesAsString() {
    StringBuilder sb = new StringBuilder(80);
    sb.append(className);
    for (Map.Entry<String, String> entry : variables.entrySet()) {
      sb.append(',').append(entry.getKey()).append(':').append(entry.getValue());
    }
    return sb.toString();
  }

  /**
   * convert a ClassAndMethods into a string that can then be used to instantiate a
   * ClassAndVariableDetails
   */
  public static String convertForStoring(ClassAndVariables cam) {
    StringBuilder sb = new StringBuilder(150);
    sb.append(cam.dclass.fullyQualifiedName());
    sb.append(',').append(cam.hasSerialVersionUID);
    if (cam.hasSerialVersionUID) {
      sb.append(',').append(cam.serialVersionUID);
    }

    List<CompiledField> fields = new ArrayList<CompiledField>(cam.variables.values());
    Collections.sort(fields);
    for (CompiledField field : fields) {
      sb.append(',').append(field.name()).append(':').append(field.descriptor());
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    return valuesAsString();
  }

  @Override
  public int compareTo(Object other) {
    return this.className.compareTo(((ClassAndVariableDetails) other).className);
  }
}
