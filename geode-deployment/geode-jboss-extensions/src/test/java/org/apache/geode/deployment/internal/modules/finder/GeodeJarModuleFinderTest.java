/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.deployment.internal.modules.finder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;

import org.jboss.modules.ConcreteModuleSpec;
import org.jboss.modules.ModuleFinder;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleLoader;
import org.jboss.modules.ModuleSpec;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.test.compiler.JarBuilder;

public class GeodeJarModuleFinderTest {

  @ClassRule
  public static TemporaryFolder stagingTempDir = new TemporaryFolder();
  private static File myJar;
  private static ModuleLoader moduleLoader;

  @BeforeClass
  public static void setup() throws IOException {
    moduleLoader = mock(ModuleLoader.class);
    myJar = new File(stagingTempDir.newFolder(), "myJar.jar");
    JarBuilder jarBuilder = new JarBuilder();
    jarBuilder.buildJarFromClassNames(myJar, "Class");
  }

  @Test
  public void testFindMatchingModuleNoDependencies() throws ModuleLoadException {
    ModuleFinder geodeJarModuleFinder =
        new GeodeJarModuleFinder("my-module", myJar.getPath(), Collections.emptyList());

    ModuleSpec moduleSpec = geodeJarModuleFinder.findModule("my-module", moduleLoader);
    ConcreteModuleSpec concreteModuleSpec = (ConcreteModuleSpec) moduleSpec;
    assertThat(concreteModuleSpec.getName()).isEqualTo("my-module");
    assertThat(concreteModuleSpec.getDependencies().length).isEqualTo(4);
  }

  @Test
  public void testFindMatchingModuleWithDependencies() throws ModuleLoadException {
    LinkedList<String> dependencies = new LinkedList<>();
    dependencies.add("my-dependency");
    ModuleFinder geodeJarModuleFinder =
        new GeodeJarModuleFinder("my-module", myJar.getPath(), dependencies);

    ModuleSpec moduleSpec = geodeJarModuleFinder.findModule("my-module", moduleLoader);
    ConcreteModuleSpec concreteModuleSpec = (ConcreteModuleSpec) moduleSpec;
    assertThat(concreteModuleSpec.getName()).isEqualTo("my-module");
    assertThat(concreteModuleSpec.getDependencies().length).isEqualTo(5);
  }

  @Test
  public void testFindNonMatchingModuleName() throws ModuleLoadException {
    ModuleFinder geodeJarModuleFinder =
        new GeodeJarModuleFinder("my-module", myJar.getPath(), Collections.emptyList());

    ModuleSpec moduleSpec = geodeJarModuleFinder.findModule("wrong-module", moduleLoader);
    assertThat(moduleSpec).isNull();
  }

  @Test
  public void testCreateFinderWithInvalidPath() {
    assertThatThrownBy(
        () -> new GeodeJarModuleFinder("my-module", "path/to/nothing.jar", Collections.emptyList()))
            .hasMessageContaining("Unable to resolve path");
  }

  @Test
  public void testCreateFinderWithNullPath() {
    assertThatThrownBy(
        () -> new GeodeJarModuleFinder("my-module", null, Collections.emptyList()))
            .hasMessageContaining("Unable to resolve path");
  }

  @Test
  public void testCreateFinderWithNullModuleName() {
    assertThatThrownBy(
        () -> new GeodeJarModuleFinder(null, myJar.getPath(), Collections.emptyList()))
            .hasMessageContaining("Module name cannot be null");
  }

  @Test
  public void testCreateFinderWithNullDependencies() throws ModuleLoadException {
    ModuleFinder geodeJarModuleFinder =
        new GeodeJarModuleFinder("my-module", myJar.getPath(), null);

    ModuleSpec moduleSpec = geodeJarModuleFinder.findModule("my-module", moduleLoader);
    ConcreteModuleSpec concreteModuleSpec = (ConcreteModuleSpec) moduleSpec;
    assertThat(concreteModuleSpec.getName()).isEqualTo("my-module");
    assertThat(concreteModuleSpec.getDependencies().length).isEqualTo(4);
  }
}
