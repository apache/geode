/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.codeAnalysis;

import static org.junit.Assert.fail;

import java.io.File;
import java.util.List;

import org.junit.Before;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import com.gemstone.gemfire.util.test.TestUtil;

/**
 * @author bruces
 * 
 */
@Category(IntegrationTest.class)
public class AnalyzeCQSerializablesJUnitTest extends AnalyzeSerializablesJUnitTest {
  
  @Before
  public void loadClasses() throws Exception {
    if (classes.size() > 0) {
      return;
    }
    System.out.println("loadClasses starting");
    List<String> excludedClasses = loadExcludedClasses(new File(TestUtil.getResourcePath(AnalyzeCQSerializablesJUnitTest.class, "excludedClasses.txt")));
    List<String> openBugs = loadOpenBugs(new File(TestUtil.getResourcePath(AnalyzeCQSerializablesJUnitTest.class, "openBugs.txt")));
    excludedClasses.addAll(openBugs);
    
    String cp = System.getProperty("java.class.path");
    System.out.println("java classpath is " + cp);
    System.out.flush();
    String[] entries = cp.split(File.pathSeparator);
    String buildDirName =
         "gemfire-cq"+File.separatorChar
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
      fail("unable to find CQ classes");
    }
  }
  
}
