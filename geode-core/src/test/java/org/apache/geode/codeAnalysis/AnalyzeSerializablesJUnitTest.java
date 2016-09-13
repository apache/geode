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
package org.apache.geode.codeAnalysis;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.codeAnalysis.decode.CompiledClass;
import org.apache.geode.codeAnalysis.decode.CompiledField;
import org.apache.geode.codeAnalysis.decode.CompiledMethod;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.util.test.TestUtil;

@Category(IntegrationTest.class)
public class AnalyzeSerializablesJUnitTest {

  /** all loaded classes */
  protected static Map<String, CompiledClass> classes = new HashMap<String, CompiledClass>();

  private static boolean ClassesNotFound;
  
  @Before
  public void loadClasses() throws Exception {
    String version = System.getProperty("java.runtime.version");
    boolean jdk18 = version != null && version.startsWith("1.8");
      // sanctioned info is based on a 1.7 compiler
    Assume.assumeTrue("AnalyzeSerializables requires a Java 8 but tests are running with v"+version, jdk18);
    if (classes.size() > 0) {
      return;
    }
    System.out.println("loadClasses starting");
    
    List<String> excludedClasses = loadExcludedClasses(new File(TestUtil.getResourcePath(AnalyzeSerializablesJUnitTest.class, "excludedClasses.txt")));
    List<String> openBugs = loadOpenBugs(new File(TestUtil.getResourcePath(AnalyzeSerializablesJUnitTest.class, "openBugs.txt")));
    excludedClasses.addAll(openBugs);
    
    String cp = System.getProperty("java.class.path");
    System.out.println("java classpath is " + cp);
    System.out.flush();
    String[] entries = cp.split(File.pathSeparator);
    String buildDirName =
         "geode-core"+File.separatorChar
        +"build"+File.separatorChar
        +"classes"+File.separatorChar
        +"main";
    String buildDir = null;
    
    for (int i=0; i<entries.length  &&  buildDir==null; i++) {
      System.out.println("examining '" + entries[i] + "'");
      System.out.flush();
      if (entries[i].endsWith(buildDirName)) {
        buildDir = entries[i];
      }
    }
    if (buildDir != null) {
      System.out.println("loading class files from " + buildDir);
      System.out.flush();
      long start = System.currentTimeMillis();
      loadClassesFromBuild(new File(buildDir), excludedClasses);
      long finish = System.currentTimeMillis();
      System.out.println("done loading " + classes.size() + " classes.  elapsed time = "
          + (finish-start)/1000 + " seconds");
    }
    else {
      fail("unable to find geode classes");
    }
  }
  
  @AfterClass
  public static void cleanup() {
    if (classes != null) {
      classes.clear();
    }
  }
  
  protected static List<String> loadExcludedClasses(File exclusionsFile) throws Exception {
    List<String> excludedClasses = new LinkedList<String>();
    FileReader fr = new FileReader(exclusionsFile);
    BufferedReader br = new BufferedReader(fr);
    try {
      String line;
      while ((line = br.readLine()) != null) {
        line = line.trim();
        if (line.length() > 0 && !line.startsWith("#")) {
          excludedClasses.add(line);
        }
      }
    } finally {
      fr.close();
    }
    return excludedClasses;
  }
  
  protected static List<String> loadOpenBugs(File exclusionsFile) throws Exception {
    List<String> excludedClasses = new LinkedList<String>();
    FileReader fr = new FileReader(exclusionsFile);
    BufferedReader br = new BufferedReader(fr);
    try {
      String line;
      // each line should have bug#,full-class-name
      while ((line = br.readLine()) != null) {
        line = line.trim();
        if (line.length() > 0 && !line.startsWith("#")) {
          String[] split = line.split(",");
          if (split.length != 2) {
            fail("unable to load classes due to misformatted line in openBugs.txt: " + line);
          }
          excludedClasses.add(line.split(",")[1].trim());
        }
      }
    } finally {
      fr.close();
    }
    return excludedClasses;
  }
  
  private static void removeExclusions(Map<String, CompiledClass> classes, List<String> exclusions) {
    for (String exclusion: exclusions) {
      exclusion = exclusion.replace('.', '/');
      classes.remove(exclusion);
    }
  }

  @Test
  public void testDataSerializables() throws Exception {
    System.out.println("testDataSerializables starting");
    if (ClassesNotFound) {
      System.out.println("... test not run due to not being able to locate product class files");
      return;
    }
    String compareToFileName = TestUtil.getResourcePath(getClass(), "sanctionedDataSerializables.txt");

    String storeInFileName = "actualDataSerializables.dat";
    File storeInFile = new File(storeInFileName);
    if (storeInFile.exists() && !storeInFile.canWrite()) {
      throw new RuntimeException("can't write " + storeInFileName);
    }
    List<ClassAndMethods> toDatas = findToDatasAndFromDatas();
    CompiledClassUtils.storeClassesAndMethods(toDatas, storeInFile);

    File compareToFile = new File(compareToFileName);
    if (!compareToFile.exists()) {
      throw new RuntimeException("can't find " + compareToFileName);
    }
    if (!compareToFile.canRead()) {
      throw new RuntimeException("can't read " + compareToFileName);
    }

    List<ClassAndMethodDetails> goldRecord = CompiledClassUtils.loadClassesAndMethods(compareToFile);
    Collections.sort(goldRecord);
    
    String diff = CompiledClassUtils.diffSortedClassesAndMethods(goldRecord, toDatas);
    if (diff.length() > 0) {
      System.out.println("++++++++++++++++++++++++++++++testDataSerializables found discrepencies++++++++++++++++++++++++++++++++++++");
      System.out.println(diff);
      fail(diff+"\n\nIf the class is not persisted or sent over the wire add it to the excludedClasses.txt file in the "
            + "\norg/apache/geode/codeAnalysis directory.  Otherwise if this doesn't "
            + "\nbreak backward compatibility move the file actualDataSerializables.dat to the codeAnalysis "
            + "\ntest directory and rename to sanctionedDataSerializables.txt");
    }
  }
  
  @Test
  public void testSerializables() throws Exception {
    System.out.println("testSerializables starting");
    System.out.flush();
    if (ClassesNotFound) {
      System.out.println("... test not run due to not being able to locate product class files");
      return;
    }
    String compareToFileName = TestUtil.getResourcePath(getClass(), "sanctionedSerializables.txt");
    File compareToFile = new File(compareToFileName);

    String storeInFileName = "actualSerializables.dat";
    File storeInFile = new File(storeInFileName);
    if (storeInFile.exists() && !storeInFile.canWrite()) {
      throw new RuntimeException("can't write " + storeInFileName);
    }
    
    List<ClassAndVariables> serializables = findSerializables();
    reset();
    CompiledClassUtils.storeClassesAndVariables(serializables, storeInFile);

    
    if (!compareToFile.exists()) {
      throw new RuntimeException("can't find " + compareToFileName);
    }
    if (!compareToFile.canRead()) {
      throw new RuntimeException("can't read " + compareToFileName);
    }
    List<ClassAndVariableDetails> goldRecord = CompiledClassUtils.loadClassesAndVariables(compareToFile);
    Collections.sort(goldRecord);
    
    String diff = CompiledClassUtils.diffSortedClassesAndVariables(goldRecord, serializables);
    classes.clear();
    if (diff.length() > 0) {
      System.out.println("++++++++++++++++++++++++++++++testSerializables found discrepencies++++++++++++++++++++++++++++++++++++");
      System.out.println(diff);
      fail(diff+"\n\nIf the class is not persisted or sent over the wire add it to the excludedClasses.txt file in the "
            + "\n/org/apache/geode/codeAnalysis/ directory.  Otherwise if this doesn't "
            + "\nbreak backward compatibility move the file actualSerializables.dat to the "
            + "\ncodeAnalysis test directory and rename to sanctionedSerializables.txt");
    }
  }
  
  /**
   * load the classes from the given files and directories
   * @param excludedClasses names of classes to exclude
   */
  public static void loadClasses(String directory, boolean recursive, List<String> excludedClasses) {
    String[] filenames = new String[]{ directory };
    List<File> classFiles = CompiledClassUtils.findClassFiles("", filenames, recursive);
    Map<String, CompiledClass> newClasses = CompiledClassUtils.parseClassFiles(classFiles);
    removeExclusions(newClasses, excludedClasses);
    classes.putAll(newClasses);
  }

  public static void loadClassesFromBuild(File buildDir, List<String> excludedClasses) {
    Map<String, CompiledClass> newClasses = CompiledClassUtils.parseClassFilesInDir(buildDir);
    removeExclusions(newClasses, excludedClasses);
    classes.putAll(newClasses);
  }
  
  /**
   * load the classes from the given jar file
   */
  public static void loadClasses(File jar, List<String> excludedClasses) {
    Map<String, CompiledClass> newClasses = CompiledClassUtils.parseClassFilesInJar(jar);
    removeExclusions(newClasses, excludedClasses);
    classes.putAll(newClasses);
  }
  
  /**
   * clears all loaded classes
   */
  public void reset() {
    classes.clear();
  }
  
  public List<ClassAndMethods> findToDatasAndFromDatas() {
    List<ClassAndMethods> result = new ArrayList<ClassAndMethods>();
    for (Map.Entry<String, CompiledClass> dentry: classes.entrySet()) {
      CompiledClass dclass = dentry.getValue();
      ClassAndMethods entry = null;
      for (int i=0; i<dclass.methods.length; i++) {
        CompiledMethod method = dclass.methods[i];
        if (!method.isAbstract() &&  method.descriptor().equals("void")) {
          String name = method.name();
          if (name.startsWith("toData") || name.startsWith("fromData")) {
            if (entry == null) {
              entry = new ClassAndMethods(dclass);
            }
            entry.methods.put(method.name(), method);
          }
        }
      }
      if (entry != null) {
        result.add(entry);
      }
    }
    Collections.sort(result);
    return result;
  }

  public List<ClassAndVariables> findSerializables() {
    List<ClassAndVariables> result = new ArrayList<ClassAndVariables>(2000);
    for (Map.Entry<String, CompiledClass> entry: classes.entrySet()) {
      CompiledClass dclass = entry.getValue();
      System.out.println("processing class " + dclass.fullyQualifiedName());
      System.out.flush();
      if (!dclass.isInterface() && dclass.isSerializableAndNotDataSerializable()) {
        ClassAndVariables cav = new ClassAndVariables(dclass);
        for (int i=0; i<dclass.fields_count; i++) {
          CompiledField f = dclass.fields[i];
          if (!f.isStatic() && !f.isTransient()) {
            cav.variables.put(f.name(), f);
          }
        }
        result.add(cav);
      }
    }
    Collections.sort(result);
    return result;
  }

}
