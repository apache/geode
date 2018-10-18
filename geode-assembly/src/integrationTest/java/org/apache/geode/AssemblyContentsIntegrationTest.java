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
package org.apache.geode;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.RestAPITest;
import org.apache.geode.util.test.TestUtil;

@Category({RestAPITest.class})
public class AssemblyContentsIntegrationTest {

  private static final String GEODE_HOME = System.getenv("GEODE_HOME");
  private Set<String> expectedAssemblyContent;

  @Before
  public void loadExpectedAssemblyContent() throws IOException {
    String assemblyContent =
        TestUtil.getResourcePath(AssemblyContentsIntegrationTest.class, "/assembly_content.txt");

    expectedAssemblyContent = Files.lines(Paths.get(assemblyContent)).collect(Collectors.toSet());
  }

  @Test
  public void verifyAssemblyContents() throws IOException {
    Set<String> currentAssemblyContent = getAssemblyContent();

    Files.write(Paths.get("assembly_content.txt"), currentAssemblyContent);

    Set<String> newAssemblyContent = new TreeSet<>(currentAssemblyContent);
    newAssemblyContent.removeAll(expectedAssemblyContent);
    Set<String> missingAssemblyContent = new TreeSet<>(expectedAssemblyContent);
    missingAssemblyContent.removeAll(currentAssemblyContent);

    String message =
        "The assembly contents have changed. Verify dependencies."
            + "\nWhen fixed, copy geode-assembly/build/integrationTest/assembly_content.txt"
            + "\nto geode-assembly/src/integrationTest/resources/assembly_content.txt"
            + "\nRemoved Content\n--------------\n"
            + String.join("\n", missingAssemblyContent) + "\n\nAdded Content\n--------------\n"
            + String.join("\n", newAssemblyContent) + "\n\n";

    assertTrue(message, expectedAssemblyContent.equals(currentAssemblyContent));
  }

  /**
   * Find all of the jars bundled with the project. Key is the name of the jar, value is the path.
   */
  private Set<String> getAssemblyContent() {
    File geodeHomeDirectory = new File(GEODE_HOME);
    Path geodeHomePath = Paths.get(GEODE_HOME);

    assertTrue(
        "Please set the GEODE_HOME environment variable to the product installation directory.",
        geodeHomeDirectory.isDirectory());

    Collection<File> contents = FileUtils.listFiles(geodeHomeDirectory, null, true);
    Set<String> sortedContent = new TreeSet<>();
    contents.forEach(content -> {
      Path path = Paths.get(content.getPath());
      // replacing '\' with '/' to test on windows properly
      sortedContent.add(geodeHomePath.relativize(path).toString().replace('\\', '/'));
    });

    return sortedContent;
  }

}
