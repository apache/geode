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
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import com.gemstone.gemfire.codeAnalysis.decode.CompiledClass;
import com.gemstone.gemfire.codeAnalysis.decode.CompiledField;
import com.gemstone.gemfire.codeAnalysis.decode.CompiledMethod;



public class CompiledClassUtils {
  /**
   * Parse the given class files and return a map of name->Dclass.  Any
   * IO exceptions are consumed by this method and written to stderr.
   * @param classFiles
   * @return the parsed classes
   */
  public static Map<String, CompiledClass> parseClassFiles(List<File> classFiles) {
    Map<String, CompiledClass> result = new HashMap<String, CompiledClass>();
    
    for (File file: classFiles) {
      try {
        CompiledClass parsed = CompiledClass.getInstance(file);
        if (!parsed.isInterface()) {
          result.put(parsed.fullyQualifiedName(), parsed);
        }
      } catch (IOException e) {
        System.err.println("Exception while parsing " + file.getName() + ": " + e.getMessage());
      }
    }
    return result;
  }
  
  /**
   * Parse the files in the given jar file and return a map of name->CompiledClass.
   * Any IO exceptions are consumed by this method and written to stderr.
   * @param jar the jar file holding classes
   */
  public static Map<String, CompiledClass> parseClassFilesInJar(File jar) {
    Map<String, CompiledClass> result = new HashMap<String, CompiledClass>();
    try {
      JarFile jarfile = new JarFile(jar);
      for (Enumeration<JarEntry> entries=jarfile.entries(); entries.hasMoreElements(); ) {
        JarEntry entry = entries.nextElement();
        if (entry.getName().endsWith(".class")) {
          try {
            CompiledClass parsed = CompiledClass.getInstance(jarfile.getInputStream(entry));
            if (!parsed.isInterface()) {
              result.put(parsed.fullyQualifiedName(), parsed);
            }
          } catch (IOException e) {
            System.err.println("Exception while parsing " + entry.getName() + ": " + e.getMessage());
          }
        }
      }
    } catch (IOException e) {
      System.err.println("Error opening jar file:");
      e.printStackTrace(System.err);
    }
    return result;
  }
  
  /**
   * Parse the files in the given jar file and return a map of name->CompiledClass.
   * Any IO exceptions are consumed by this method and written to stderr.
   */
  public static Map<String, CompiledClass> parseClassFilesInDir(File buildDir) {
    Map<String, CompiledClass> result = new HashMap<String, CompiledClass>();
    for (File entry: buildDir.listFiles()) {
      if (entry.isDirectory()) {
        result.putAll(parseClassFilesInDir(entry));
      }
      else if (entry.getName().endsWith(".class")) {
        try {
          CompiledClass parsed = CompiledClass.getInstance(new FileInputStream(entry));
          if (!parsed.isInterface()) {
            result.put(parsed.fullyQualifiedName(), parsed);
          }
        } catch (IOException e) {
          System.err.println("Exception while parsing " + entry.getName() + ": " + e.getMessage());
        }
      }
    }
    return result;
  }
  
  /**
   * returns a collection of all of the .class files in the given list
   * of files and directories.
   * 
   * @param filenames a list of the files and directories to examine
   * @param recursive whether to recurse into subdirectories
   * @return a sorted list of the .class files found
   */
  public static List<File> findClassFiles(String parentPath, String[] filenames, boolean recursive) {
    // Grab classes and Expand directory names found in list
    List<File> classFiles = new ArrayList<File>();
    for (int i = 0; i <  filenames.length; i ++) {
      File f = new File(parentPath+filenames[i]);
      String n = f.getAbsolutePath();
      if (!f.exists()) {
        System.err.println("File " + n + " does not exist - skipping");
        continue;
      }
      if (f.isFile() && f.getName().endsWith(".class")) {
        classFiles.add(f);
        continue;
      }
      if (f.isDirectory() && recursive) {
        classFiles.addAll(findClassFiles(f.getAbsolutePath()+"/", f.list(), true));
      }
    }
    Collections.sort(classFiles);
    return classFiles;
  }

  public static List<ClassAndMethodDetails> loadClassesAndMethods(File file) throws IOException {
    List<ClassAndMethodDetails> result = new LinkedList<ClassAndMethodDetails>();
    FileReader fr = new FileReader(file);
    LineNumberReader in = new LineNumberReader(fr);
    ClassAndMethodDetails cam;
    while ((cam = ClassAndMethodDetails.create(in)) != null) {
      result.add(cam);
    }
    fr.close();
    return result;
  }

  public static String diffSortedClassesAndMethods(
      List<ClassAndMethodDetails> goldRecord, List<ClassAndMethods> toDatas) throws IOException {
    
    StringBuilder newClassesSb = new StringBuilder(10000);
    StringBuilder changedClassesSb = new StringBuilder(10000);
    newClassesSb.append("New or moved classes----------------------------------------\n");
    int newBase = newClassesSb.length();
    changedClassesSb.append("Modified classes--------------------------------------------\n");
    int changedBase = changedClassesSb.length();
    
    Iterator<ClassAndMethods> it = toDatas.iterator();
    ClassAndMethods newclass = null;
    
    for (ClassAndMethodDetails gold: goldRecord) {
      if (newclass == null) {
        if (!it.hasNext()) {
          changedClassesSb.append(gold).append(": deleted or moved\n");
          continue;
        }
        newclass = it.next();
      }
      int comparison = -1;
      while (newclass != null
          && (comparison=gold.className.compareTo(newclass.dclass.fullyQualifiedName())) > 0) {
        newClassesSb.append(newclass).append("\n");
        if (it.hasNext()) {
          newclass = it.next();
        } else {
          newclass = null;
        }
      }
      if (comparison == 0) {
        ClassAndMethods nc = newclass;
        newclass = null;
        if (gold.methodCode.size() != nc.numMethods()) {
          changedClassesSb.append(nc).append(": method count\n");
          continue;
        }
        boolean comma = false;
        for (Map.Entry<String, CompiledMethod> entry: nc.methods.entrySet()) {
          CompiledMethod method = entry.getValue();
          String name = method.name();
          byte[] goldCode = gold.methodCode.get(name);
          if (goldCode == null) {
            if (comma) {
              changedClassesSb.append(", and ");
            } else {
              changedClassesSb.append(nc).append(":  ");
              comma = true;
            }
            changedClassesSb.append(name).append(" was added");
            continue; // only report one diff per class
          }
          String diff;
          if ((diff = codeDiff(goldCode, method.getCode().code)) != null) {
            if (comma) {
              changedClassesSb.append(", and ");
            } else {
              changedClassesSb.append(nc).append(":  ");
              comma = true;
            }
            changedClassesSb.append(name).append(diff);
            continue;
          }
        }
        for (Map.Entry<String, byte[]> entry: gold.methodCode.entrySet()) {
          if (!nc.methods.containsKey(entry.getKey())) {
            if (comma) {
              changedClassesSb.append(", and ");
            } else {
              changedClassesSb.append(nc).append(":  ");
            }
            changedClassesSb.append(entry.getKey()).append(" is missing");
          }
        }
        if (comma) {
          changedClassesSb.append("\n");
        }
      }
    }
    while (it.hasNext()) {
      newclass = it.next();
      newClassesSb.append(newclass).append(": new class\n");
    }
    String result = "";
    if (newClassesSb.length() > newBase) {
      if (changedClassesSb.length() > changedBase) {
        newClassesSb.append("\n");
        newClassesSb.append(changedClassesSb);
      }
      result = newClassesSb.toString();
    } else if (changedClassesSb.length() > changedBase) {
      result = changedClassesSb.toString();
    }
    return result;
  }

  
  
  public static void storeClassesAndMethods(List<ClassAndMethods> cams,
      File file) throws IOException {
    FileWriter fw = new FileWriter(file);
    BufferedWriter out = new BufferedWriter(fw);
    for (ClassAndMethods entry: cams) {
      out.append(ClassAndMethodDetails.convertForStoring(entry));
      out.newLine();
    }
    out.flush();
    out.close();
  }

  static String codeDiff(byte[] method1, byte[] method2) {
    if (method1.length != method2.length) {
      return " (len="+method2.length+",expected="+method1.length+")";
    }
//    for (int i=0; i<method1.length; i++) {
//      if (method1[i] != method2[i]) {
//        return "(code["+i+"])";
//      }
//    }
    return null;
  }








  public static List<ClassAndVariableDetails> loadClassesAndVariables(File file) throws IOException {
    List<ClassAndVariableDetails> result = new LinkedList<ClassAndVariableDetails>();
    FileReader fr = new FileReader(file);
    BufferedReader in = new BufferedReader(fr);
    String line;
    while ((line = in.readLine()) != null) {
      line = line.trim();
      if (line.startsWith("#") || line.startsWith("//")) {
        // comment line
      }
      else {
        result.add(new ClassAndVariableDetails(line));
      }
    }
    fr.close();
    return result;
  }

  public static String diffSortedClassesAndVariables(
      List<ClassAndVariableDetails> goldRecord, List<ClassAndVariables> cavs) throws IOException {
    
    StringBuilder newClassesSb = new StringBuilder(10000);
    StringBuilder changedClassesSb = new StringBuilder(10000);
    newClassesSb.append("New or moved classes----------------------------------------\n");
    int newBase = newClassesSb.length();
    changedClassesSb.append("Modified classes--------------------------------------------\n");
    int changedBase = changedClassesSb.length();
    
    Iterator<ClassAndVariables> it = cavs.iterator();
    ClassAndVariables newclass = null;
    
    List<String> added = new ArrayList<String>();
    List<String> removed = new ArrayList<String>();
    List<String> changed = new ArrayList<String>();
    
    for (ClassAndVariableDetails gold: goldRecord) {
      added.clear(); removed.clear(); changed.clear();
      if (newclass == null) {
        if (!it.hasNext()) {
          changedClassesSb.append(gold).append(": deleted or moved\n");
          continue;
        }
        newclass = it.next();
      }
      int comparison = -1;
      while (newclass != null
          && (comparison=gold.className.compareTo(newclass.dclass.fullyQualifiedName())) > 0) {
        newClassesSb.append(ClassAndVariableDetails.convertForStoring(newclass)).append("\n");
        newclass = null;
        if (it.hasNext()) {
          newclass = it.next();
        }
      }
      if (comparison == 0) {
        ClassAndVariables nc = newclass;
        newclass = null;
//        if (gold.variables.size() != nc.variables.size()) {
//          changedClassesSb.append(nc).append(": field count\n");
//          continue;
//        }
        for (Map.Entry<String, CompiledField> entry: nc.variables.entrySet()) {
          CompiledField field = entry.getValue();
          String name = entry.getKey();
          String type = gold.variables.get(name);
          if (type == null) {
//            if (comma) {
//              changedClassesSb.append(", and ");
//            } else {
//              changedClassesSb.append(nc).append(":  ");
//              comma = true;
//            }
            added.add(name);
            continue; // only report one diff per class
          }
          String newType = field.descriptor();
          if (!newType.equals(type)) {
            changed.add(name + " type changed to " + newType);
            continue;
          }
        }
        for (Map.Entry<String, String> entry: gold.variables.entrySet()) {
          if (!nc.variables.containsKey(entry.getKey())) {
            removed.add(entry.getKey());
          }
        }
        if (!(added.isEmpty() && removed.isEmpty() && changed.isEmpty())) {
          changedClassesSb.append(nc).append('\n');
        }
        if (!added.isEmpty()) {
          changedClassesSb.append("\t\t added fields: ").append(added).append('\n');
        }
        if (!changed.isEmpty()) {
          changedClassesSb.append("\t\t changed fields: ").append(changed).append('\n');
        }
        if (gold.hasSerialVersionUID) {
          if (nc.hasSerialVersionUID) {
            if (!Long.valueOf(gold.serialVersionUID).equals(nc.serialVersionUID)) {
              changedClassesSb.append("\t\t " + nc.dclass.fullyQualifiedName() + " serialVersionUID was changed from " + gold.serialVersionUID + " to " + nc.serialVersionUID + " this may break client/server compatibility as well as server/server compatibility \n");
            }
          }
          else {
            changedClassesSb.append("\t\t " + nc.dclass.fullyQualifiedName() + " serialVersionUID was removed, this may break client/server compatibility as well as server/server compatibility \n");
          }
        }
        else {
          if (nc.hasSerialVersionUID) {
            changedClassesSb.append("\t\t " + nc.dclass.fullyQualifiedName() + " serialVersionUID was added \n");
          }
        }
      }
    }
    while (it.hasNext()) {
      newclass = it.next();
      newClassesSb.append(ClassAndVariableDetails.convertForStoring(newclass)).append(": new class\n");
    }
    String result = "";
    if (newClassesSb.length() > newBase) {
      if (changedClassesSb.length() > changedBase) {
        newClassesSb.append("\n");
        newClassesSb.append(changedClassesSb);
      }
      result = newClassesSb.toString();
    } else if (changedClassesSb.length() > changedBase) {
      result = changedClassesSb.toString();
    }
    return result;
  }

  
  
  public static void storeClassesAndVariables(List<ClassAndVariables> cams,
      File file) throws IOException {
    FileWriter fw = new FileWriter(file);
    BufferedWriter out = new BufferedWriter(fw);
    for (ClassAndVariables entry: cams) {
      out.append(ClassAndVariableDetails.convertForStoring(entry));
      out.newLine();
    }
    out.flush();
    out.close();
  }

}
