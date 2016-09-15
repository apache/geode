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
package org.apache.geode;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.jar.JarFile;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.FileUtil;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.util.test.TestUtil;

@Category(IntegrationTest.class)
public class BundledJarsJUnitTest {

  private static final String VERSION_PATTERN = "[0-9-_.v]{3,}.*\\.jar$";
  protected static final String GEMFIRE_HOME = System.getenv("GEMFIRE");
  private Set<String> expectedJars;
  
  @Before
  public void loadExpectedJars() throws IOException {
    String expectedJarFile = TestUtil.getResourcePath(BundledJarsJUnitTest.class, "/expected_jars.txt");
    
    expectedJars = Files.lines(Paths.get(expectedJarFile))
        .collect(Collectors.toSet());
  }
  
  @Test
  public void verifyBundledJarsHaveNotChanged() throws IOException {
    TreeMap<String, String> sortedJars = getBundledJars();
    Stream<String> lines = sortedJars.entrySet().stream().map(entry -> removeVersion(entry.getKey()));
    Set<String> bundledJarNames = new TreeSet<String>(lines.collect(Collectors.toSet()));
    
    Files.write(Paths.get("bundled_jars.txt"), bundledJarNames);

    TreeSet<String> newJars = new TreeSet<String>(bundledJarNames);
    newJars.removeAll(expectedJars);
    TreeSet<String> missingJars = new TreeSet<String>(expectedJars);
    missingJars.removeAll(bundledJarNames);
    
    StringBuilder message = new StringBuilder();
    message.append("The bundled jars have changed. Please make sure you update the licence and notice");
    message.append("\nas described in https://cwiki.apache.org/confluence/display/GEODE/License+Guide+for+Contributors");
    message.append("\nWhen fixed, copy geode-assembly/build/test/bundled_jars.txt");
    message.append("\nto src/test/resources/expected_jars.txt");
    message.append("\nRemoved Jars\n--------------\n");
    message.append(String.join("\n", missingJars));
    message.append("\n\nAdded Jars\n--------------\n");
    message.append(String.join("\n", newJars));
    message.append("\n\n");
    
    assertTrue(message.toString(), expectedJars.equals(bundledJarNames));
    
  }

  /**
   * Find all of the jars bundled with the project.
   * Key is the name of the jar, value is the path.
   */
  protected TreeMap<String, String> getBundledJars() {
    File gemfireHomeDirectory= new File(GEMFIRE_HOME);

    assertTrue("Please set the GEMFIRE environment variable to the product installation directory.",
        gemfireHomeDirectory.isDirectory());
    
    List<File> jars = FileUtil.findAll(gemfireHomeDirectory, ".*\\.jar");
    TreeMap<String, String> sortedJars = new TreeMap<String, String>();
    jars.stream().forEach(jar -> sortedJars.put(jar.getName(), jar.getPath()));
    
    List<File> wars = FileUtil.findAll(gemfireHomeDirectory, ".*\\.war");
    TreeSet<File> sortedWars = new TreeSet<File>(wars);
    sortedWars.stream().flatMap(BundledJarsJUnitTest::extractJarNames)
       .forEach(jar -> sortedJars.put(jar.getName(), jar.getPath()));
    
    sortedJars.keySet().removeIf(s -> s.startsWith("geode"));
    return sortedJars;
  }
  
  private String removeVersion(String name) {
    return name.replaceAll(VERSION_PATTERN, "");
  }

  /**
   * Find of of the jar files embedded within a war
   */
  private static Stream<File> extractJarNames(File war) {
    try (JarFile warContents = new JarFile(war)) {
      return warContents.stream()
           //Look for jars in the war
          .filter(entry -> entry.getName().endsWith(".jar"))
          //Create a File with a path that includes the war name
          .map(entry -> new File(war.getName(), entry.getName()))
          //Materialize the list of files while the war is still open
          .collect(Collectors.toList()).stream();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
