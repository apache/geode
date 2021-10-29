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

package org.apache.geode.management.internal.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.junit.Test;


public class GemfireCoreClasspathTest {
  @Test
  public void testGemFireCoreClasspath() throws IOException {
    File coreDependenciesJar = new File(StartMemberUtils.CORE_DEPENDENCIES_JAR_PATHNAME);
    assertThat(coreDependenciesJar).as(coreDependenciesJar + " is not a file").isFile();
    Collection<String> expectedJarDependencies =
        Arrays.asList("antlr", "commons-io", "commons-lang", "commons-logging", "geode",
            "jackson-annotations", "jackson-core", "jackson-databind", "jline", "snappy",
            "spring-core", "spring-shell", "jetty-server", "jetty-servlet", "jetty-webapp",
            "jetty-util", "jetty-http", "servlet-api", "jetty-io", "jetty-security", "jetty-xml");
    assertJarFileManifestClassPath(coreDependenciesJar, expectedJarDependencies);
  }

  private void assertJarFileManifestClassPath(final File dependenciesJar,
      final Collection<String> expectedJarDependencies) throws IOException {
    JarFile dependenciesJarFile = new JarFile(dependenciesJar);
    Manifest manifest = dependenciesJarFile.getManifest();

    assertThat(manifest).isNotNull();

    Attributes attributes = manifest.getMainAttributes();

    assertThat(attributes).containsKey(Attributes.Name.CLASS_PATH);

    String[] actualJarDependencies = attributes.getValue(Attributes.Name.CLASS_PATH).split(" ");

    assertThat(actualJarDependencies).hasSizeGreaterThanOrEqualTo(expectedJarDependencies.size());

    assertThat(actualJarDependencies)
        .as("GemFire dependencies JAR file does not contain the expected dependencies in the Manifest Class-Path attribute")
        .containsAll(expectedJarDependencies);
  }
}
