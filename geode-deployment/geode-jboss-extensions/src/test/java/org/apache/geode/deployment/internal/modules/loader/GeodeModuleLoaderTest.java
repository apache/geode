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
package org.apache.geode.deployment.internal.modules.loader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import org.jboss.modules.Module;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleNotFoundException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.deployment.internal.modules.finder.GeodeCompositeModuleFinder;
import org.apache.geode.deployment.internal.modules.finder.GeodeJarModuleFinder;
import org.apache.geode.test.compiler.JarBuilder;

public class GeodeModuleLoaderTest {

  @ClassRule
  public static TemporaryFolder stagingTempDir = new TemporaryFolder();
  private static File myJar;
  private GeodeModuleLoader geodeModuleLoader;

  @BeforeClass
  public static void setupClass() throws IOException {
    JarBuilder jarBuilder = new JarBuilder();
    myJar = new File(stagingTempDir.newFolder(), "myJar.jar");
    jarBuilder.buildJarFromClassNames(myJar, "Class1");
  }

  @Before
  public void setup() throws ModuleLoadException {
    GeodeCompositeModuleFinder compositeModuleFinder = new GeodeCompositeModuleFinder();
    GeodeJarModuleFinder geodeCustomModuleFinder =
        new GeodeJarModuleFinder("geode-custom-jar-deployments",
            myJar.getPath(), Collections.emptyList());
    GeodeJarModuleFinder geodeCoreModuleFinder = new GeodeJarModuleFinder("geode-core",
        myJar.getPath(), Collections.emptyList());
    compositeModuleFinder.addModuleFinder("geode-custom-jar-deployments", geodeCustomModuleFinder);
    compositeModuleFinder.addModuleFinder("geode-core", geodeCoreModuleFinder);
    geodeModuleLoader = new GeodeModuleLoader(compositeModuleFinder);
    geodeModuleLoader.loadModule("geode-core");
  }

  @Test
  public void testLoadModuleWithNull() {
    String name = null;
    assertThatThrownBy(() -> geodeModuleLoader.loadModule(name))
        .hasMessageContaining("Module name cannot be null");
  }

  @Test
  public void testLoadModuleWithNoModulesLoaded() {
    assertThatThrownBy(() -> geodeModuleLoader.loadModule("my-module"))
        .isInstanceOf(ModuleNotFoundException.class).hasMessageContaining("my-module");
  }

  @Test
  public void testPreloadModuleWithNull() {
    assertThatThrownBy(() -> geodeModuleLoader.preloadModule(null))
        .hasMessageContaining("Module name cannot be null");
  }

  @Test
  public void testPreloadModuleWithNoModulesLoaded() throws ModuleLoadException {
    assertThat(geodeModuleLoader.preloadModule("my-module")).isNull();
  }

  @Test
  public void testRegisterModule() throws ModuleLoadException {
    geodeModuleLoader.registerModule("my-module", myJar.getPath(), Collections.emptyList());
    Module module = geodeModuleLoader.loadModule("my-module");
    assertThat(module.getName()).isEqualTo("my-module");
    assertThat(module.getDependencies().length).isEqualTo(4);
  }

  @Test
  public void testRegisterModuleWithDependencies() throws ModuleLoadException {
    geodeModuleLoader
        .registerModule("other-module", myJar.getPath(), Collections.emptyList());
    geodeModuleLoader
        .registerModule("my-module", myJar.getPath(), Collections.singletonList("other-module"));
    Module module = geodeModuleLoader.loadModule("my-module");
    assertThat(module.getName()).isEqualTo("my-module");
    assertThat(module.getDependencies().length).isEqualTo(5);
  }

  @Test
  public void testRegisterModuleWithInvalidDependencies() throws ModuleLoadException {
    geodeModuleLoader
        .registerModule("my-module", myJar.getPath(), Collections.singletonList("other-module"));
    assertThatThrownBy(() -> geodeModuleLoader.loadModule("my-module")).isInstanceOf(
        ModuleNotFoundException.class).hasMessageContaining("other-module");
  }

  @Test
  public void testRegisterModuleWithNullDependencies() throws ModuleLoadException {
    geodeModuleLoader
        .registerModule("my-module", myJar.getPath(), null);
    Module module = geodeModuleLoader.loadModule("my-module");
    assertThat(module.getName()).isEqualTo("my-module");
    assertThat(module.getDependencies().length).isEqualTo(4);
  }

  @Test
  public void testRegisterModuleWithNullName() {
    assertThatThrownBy(
        () -> geodeModuleLoader.registerModule(null, myJar.getPath(), Collections.emptyList()))
            .hasMessageContaining("Module name cannot be null");
  }

  @Test
  public void testLoadModuleRegisteredWithInvalidDependency() throws ModuleLoadException {
    geodeModuleLoader
        .registerModule("my-module", myJar.getPath(), Collections.singletonList("other-module"));
    assertThatThrownBy(() -> geodeModuleLoader.loadModule("my-module"))
        .isInstanceOf(ModuleNotFoundException.class).hasMessageContaining("other-module");
  }

  @Test
  public void testRegisterModuleWithNullPath() {
    assertThatThrownBy(
        () -> geodeModuleLoader.registerModule("my-module", null, Collections.emptyList()))
            .hasMessageContaining("Unable to resolve path to jar");
  }

  @Test
  public void testRegisterModuleWithGeodeInName() {
    assertThatThrownBy(
        () -> geodeModuleLoader
            .registerModule("geode-module", myJar.getPath(), Collections.emptyList()))
                .hasMessageContaining("Deployments starting with \"geode-\" are not allowed");
  }

  @Test
  public void testUnregisterModule() throws ModuleLoadException {
    geodeModuleLoader
        .registerModule("my-module", myJar.getPath(), Collections.emptyList());
    assertThat(geodeModuleLoader.loadModule("my-module")).isNotNull();
    geodeModuleLoader.unregisterModule("my-module");
    assertThat(geodeModuleLoader.preloadModule("my-module")).isNull();

  }

  @Test
  public void testUnregisterInvalidModule() throws ModuleLoadException {
    geodeModuleLoader
        .registerModule("my-module", myJar.getPath(), Collections.emptyList());
    assertThat(geodeModuleLoader.loadModule("my-module")).isNotNull();
    geodeModuleLoader.unregisterModule("wrong-module");
    assertThat(geodeModuleLoader.loadModule("my-module")).isNotNull();
  }

  @Test
  public void testUnregisterNullModule() {
    assertThatThrownBy(() -> geodeModuleLoader.unregisterModule(null))
        .hasMessageContaining("Module name cannot be null");
  }

  @Test
  public void testUnRegisterModuleWithGeodeInName() {
    assertThatThrownBy(() -> geodeModuleLoader.unregisterModule("geode-module"))
        .hasMessageContaining("Deployments starting with \"geode-\" are not allowed");
  }

  @Test
  public void testUnregisterModuleDependencyOfOtherModule() throws ModuleLoadException {
    geodeModuleLoader
        .registerModule("other-module", myJar.getPath(), Collections.emptyList());
    geodeModuleLoader
        .registerModule("my-module", myJar.getPath(), Collections.singletonList("other-module"));
    assertThat(geodeModuleLoader.loadModule("my-module").getDependencies().length).isEqualTo(5);
    geodeModuleLoader.unregisterModule("other-module");
    assertThat(geodeModuleLoader.loadModule("my-module").getDependencies().length).isEqualTo(4);
    assertThatThrownBy(() -> geodeModuleLoader.loadModule("other-module"))
        .isInstanceOf(ModuleNotFoundException.class).hasMessageContaining("other-module");
  }

  @Test
  public void testRegisterModuleAsDependencyOfModuleNeitherExist() {
    assertThatThrownBy(
        () -> geodeModuleLoader.registerModulesAsDependencyOfModule("my-module", "other-module"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("No such module: my-module");
  }

  @Test
  public void testRegisterInvalidModuleAsDependencyOfModule() throws ModuleLoadException {
    geodeModuleLoader.registerModule("my-module", myJar.getPath(), Collections.emptyList());
    geodeModuleLoader.loadModule("my-module");
    assertThatThrownBy(
        () -> geodeModuleLoader.registerModulesAsDependencyOfModule("my-module", "other-module"))
            .isInstanceOf(ModuleNotFoundException.class)
            .hasMessageContaining("other-module");
  }

  @Test
  public void testRegisterModuleAsDependencyOfInvalidModule() throws ModuleLoadException {
    geodeModuleLoader.registerModule("other-module", myJar.getPath(), Collections.emptyList());
    geodeModuleLoader.loadModule("other-module");
    assertThatThrownBy(
        () -> geodeModuleLoader.registerModulesAsDependencyOfModule("my-module", "other-module"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("No such module: my-module");
  }

  @Test
  public void testRegisterModuleAsDependencyOfModule() throws ModuleLoadException {
    geodeModuleLoader.registerModule("other-module", myJar.getPath(), Collections.emptyList());
    geodeModuleLoader.registerModule("my-module", myJar.getPath(), Collections.emptyList());
    assertThat(geodeModuleLoader.loadModule("my-module").getDependencies().length).isEqualTo(4);
    geodeModuleLoader.registerModulesAsDependencyOfModule("my-module", "other-module");
    assertThat(geodeModuleLoader.loadModule("my-module").getDependencies().length).isEqualTo(5);
  }

  @Test
  public void testRegisterModuleAsDependencyOfNullModule() {
    assertThatThrownBy(
        () -> geodeModuleLoader.registerModulesAsDependencyOfModule(null, "other-module"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Module name cannot be null");
  }

  @Test
  public void testRegisterNullModuleAsDependencyOfModule() {
    assertThatThrownBy(
        () -> geodeModuleLoader.registerModulesAsDependencyOfModule("my-module", null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Modules to depend on cannot be null");
  }

  @Test
  public void testUnregisterAddedModuleDependencyFromModules() throws ModuleLoadException {
    geodeModuleLoader.registerModule("other-module", myJar.getPath(), Collections.emptyList());
    geodeModuleLoader.registerModule("my-module", myJar.getPath(), Collections.emptyList());
    geodeModuleLoader.loadModule("my-module");
    geodeModuleLoader.registerModulesAsDependencyOfModule("my-module", "other-module");
    assertThat(geodeModuleLoader.loadModule("my-module").getDependencies().length).isEqualTo(5);
    geodeModuleLoader.unregisterModuleDependencyFromModules("other-module");
    assertThat(geodeModuleLoader.loadModule("my-module").getDependencies().length).isEqualTo(4);
    assertThat(geodeModuleLoader.loadModule("other-module")).isNotNull();
  }

  @Test
  public void testUnregisterModuleDependencyFromModules() throws ModuleLoadException {
    geodeModuleLoader.registerModule("other-module", myJar.getPath(), Collections.emptyList());
    geodeModuleLoader
        .registerModule("my-module", myJar.getPath(), Collections.singletonList("other-module"));
    assertThat(geodeModuleLoader.loadModule("my-module").getDependencies().length).isEqualTo(5);
    geodeModuleLoader.unregisterModuleDependencyFromModules("other-module");
    assertThat(geodeModuleLoader.loadModule("my-module").getDependencies().length).isEqualTo(4);
    assertThat(geodeModuleLoader.loadModule("other-module")).isNotNull();
  }

  @Test
  public void testUnregisterInvalidModuleDependencyFromModules() throws ModuleLoadException {
    geodeModuleLoader.registerModule("my-module", myJar.getPath(), Collections.emptyList());
    assertThat(geodeModuleLoader.loadModule("my-module").getDependencies().length).isEqualTo(4);
    geodeModuleLoader.unregisterModuleDependencyFromModules("wrong-module");
    assertThat(geodeModuleLoader.loadModule("my-module").getDependencies().length).isEqualTo(4);
  }

  @Test
  public void testUnregisterNullModuleDependencyFromModules() {
    assertThatThrownBy(() -> geodeModuleLoader.unregisterModuleDependencyFromModules(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Module dependency name cannot be null");
  }
}
