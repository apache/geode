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

import static org.junit.Assert.fail;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.experimental.categories.Category;

import org.apache.geode.codeAnalysis.decode.CompiledClass;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.util.test.TestUtil;

/**
 * 
 */
@Category(IntegrationTest.class)
public class AnalyzeWANSerializablesJUnitTest extends AnalyzeSerializablesJUnitTest {

  @Before
  public void loadClasses() throws Exception {
    if (classes.size() > 0) {
      return;
    }
    System.out.println("loadClasses starting");
    List<String> excludedClasses = loadExcludedClasses(new File(
        TestUtil.getResourcePath(AnalyzeWANSerializablesJUnitTest.class, "excludedClasses.txt")));
    List<String> openBugs = loadOpenBugs(
        new File(TestUtil.getResourcePath(AnalyzeWANSerializablesJUnitTest.class, "openBugs.txt")));
    excludedClasses.addAll(openBugs);

    String cp = System.getProperty("java.class.path");
    System.out.println("java classpath is " + cp);
    System.out.flush();
    String[] entries = cp.split(File.pathSeparator);
    String buildDirName = "geode-wan" + File.separatorChar + "build" + File.separatorChar
        + "classes" + File.separatorChar + "main";
    String buildDir = null;

    for (int i = 0; i < entries.length && buildDir == null; i++) {
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
          + (finish - start) / 1000 + " seconds");
    } else {
      fail("unable to find WAN classes");
    }
  }

  @AfterClass
  public static void cleanup() {
    if (classes != null) {
      classes.clear();
    }
  }

}
