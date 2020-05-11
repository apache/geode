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

import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.RestAPITest;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;

@Category(RestAPITest.class)
public class AssemblyContentsIntegrationTest {

  private static final String GEODE_HOME = System.getenv("GEODE_HOME");

  private Collection<String> expectedAssemblyContent;

  @Rule
  public RequiresGeodeHome requiresGeodeHome = new RequiresGeodeHome();

  @Before
  public void loadExpectedAssemblyContent() throws IOException {
    String assemblyContent =
        createTempFileFromResource(AssemblyContentsIntegrationTest.class,
            "/assembly_content.txt").getAbsolutePath();

    expectedAssemblyContent =
        Files.lines(Paths.get(assemblyContent)).collect(Collectors.toCollection(TreeSet::new));
  }

  @Test
  public void verifyAssemblyContents() throws IOException {
    Collection<String> currentAssemblyContent = getAssemblyContent();
    Files.write(Paths.get("assembly_content.txt"), currentAssemblyContent);

    assertThat(currentAssemblyContent)
        .as("The assembly contents have changed. Verify dependencies and "
            + "copy geode-assembly/build/integrationTest/assembly_content.txt to "
            + "geode-assembly/src/integrationTest/resources/assembly_content.txt")
        .containsExactlyElementsOf(expectedAssemblyContent);
  }


  /**
   * Find all of the jars bundled with the project. Key is the name of the jar, value is the path.
   */
  private Collection<String> getAssemblyContent() {
    File geodeHomeDirectory = new File(GEODE_HOME);
    Path geodeHomePath = Paths.get(GEODE_HOME);

    assertThat(geodeHomeDirectory)
        .as(
            "Please set the GEODE_HOME environment variable to the product installation directory.")
        .isDirectory();

    String versionRegex = "\\d+\\.\\d+\\.\\d+(-build\\.\\d+)?";
    return FileUtils.listFiles(geodeHomeDirectory, null, true).stream()
        .map(file -> geodeHomePath.relativize(Paths.get(file.getPath())).toString().replace('\\',
            '/'))
        .map(entry -> entry.contains("/geode-")
            ? entry.replaceFirst(versionRegex, "0.0.0") : entry)
        .map(entry -> entry.contains("Apache_Geode")
            ? entry.replaceFirst(versionRegex, "0.0.0") : entry)
        .collect(Collectors.toCollection(TreeSet::new));
  }
}
