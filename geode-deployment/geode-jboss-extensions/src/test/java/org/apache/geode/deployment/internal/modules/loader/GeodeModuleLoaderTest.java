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

import java.util.Arrays;
import java.util.stream.Collectors;

import org.jboss.modules.Module;
import org.jboss.modules.ModuleDependencySpecBuilder;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleNotFoundException;
import org.jboss.modules.ModuleSpec;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.deployment.internal.modules.finder.GeodeDelegatingModuleFinder;

public class GeodeModuleLoaderTest {
  private static GeodeDelegatingModuleFinder delegatingModuleFinder;

  private GeodeModuleLoader geodeModuleLoader;

  @Before
  public void setup() throws ModuleLoadException {
    delegatingModuleFinder = new GeodeDelegatingModuleFinder();
    addModuleToFinder("geode");
    geodeModuleLoader = new GeodeModuleLoader(delegatingModuleFinder);
    geodeModuleLoader.loadModule("geode");
  }

  @Test
  public void testLoadModuleWithNull() throws ModuleLoadException {
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
  public void testRegisterModule() throws ModuleLoadException {
    addModuleToFinder("my-module");
    geodeModuleLoader.linkModules("geode", "my-module", true);
    Module module = geodeModuleLoader.loadModule("my-module");
    assertThat(module.getName()).isEqualTo("my-module");
    assertThat(Arrays.stream(module.getDependencies())
        .map(Object::toString).collect(Collectors.toList()))
            .containsExactlyInAnyOrder("dependency on java.base");
  }

  @Test
  public void testLoadModuleWithDependencies() throws ModuleLoadException {
    addModuleToFinder("other-module");
    addModuleToFinder("my-module", "other-module");
    Module module = geodeModuleLoader.loadModule("my-module");
    assertThat(module.getName()).isEqualTo("my-module");
    assertThat(Arrays.stream(geodeModuleLoader.loadModule("my-module").getDependencies())
        .map(Object::toString).collect(Collectors.toList()))
            .containsExactlyInAnyOrder("dependency on java.base",
                "dependency on other-module");
  }

  @Test
  public void testLinkModulesWithNullName() throws ModuleLoadException {
    addModuleToFinder("my-module");
    assertThatThrownBy(
        () -> geodeModuleLoader.linkModules(null, "my-module", true))
            .hasMessageContaining("Module name cannot be null");
  }

  @Test
  public void testLinkModulesWithNullDependee() throws ModuleLoadException {
    addModuleToFinder("my-module");
    assertThatThrownBy(
        () -> geodeModuleLoader.linkModules("my-module", null, true))
            .hasMessageContaining("Module to depend on cannot be null");
  }

  @Test
  public void testLinkModulesWithInvalidDependee() {
    addModuleToFinder("my-module");
    assertThatThrownBy(() -> geodeModuleLoader
        .linkModules("my-module", "other-module", true))
            .isInstanceOf(ModuleNotFoundException.class).hasMessageContaining("other-module");
  }

  @Test
  public void testUnregisterModule() throws ModuleLoadException {
    addModuleToFinder("my-module");
    assertThat(geodeModuleLoader.loadModule("my-module")).isNotNull();
    geodeModuleLoader.unregisterModule("my-module");
    assertThatThrownBy(() -> geodeModuleLoader.loadModule("my-module"))
        .isInstanceOf(ModuleNotFoundException.class).hasMessageContaining("my-module");

  }

  @Test
  public void testUnregisterInvalidModule() throws ModuleLoadException {
    addModuleToFinder("my-module");
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
  public void testUnregisterModuleDependencyOfOtherModule() throws ModuleLoadException {
    addModuleToFinder("other-module");
    addModuleToFinder("my-module", "other-module");
    assertThat(Arrays.stream(geodeModuleLoader.loadModule("my-module").getDependencies())
        .map(Object::toString).collect(Collectors.toList()))
            .containsExactlyInAnyOrder("dependency on java.base",
                "dependency on other-module");
    geodeModuleLoader.unregisterModule("other-module");
    assertThat(Arrays.stream(geodeModuleLoader.loadModule("my-module").getDependencies())
        .map(Object::toString).collect(Collectors.toList()))
            .containsExactlyInAnyOrder("dependency on java.base");
    assertThatThrownBy(() -> geodeModuleLoader.loadModule("other-module"))
        .isInstanceOf(ModuleNotFoundException.class).hasMessageContaining("other-module");
  }

  @Test
  public void testRegisterModuleAsDependencyOfModuleNeitherExist() {
    assertThatThrownBy(
        () -> geodeModuleLoader.linkModules("my-module", "other-module",
            true))
                .isInstanceOf(ModuleNotFoundException.class)
                .hasMessageContaining("my-module");
  }

  @Test
  public void testRegisterModuleAsDependencyOfModule() throws ModuleLoadException {
    addModuleToFinder("other-module");
    addModuleToFinder("my-module");
    assertThat(Arrays.stream(geodeModuleLoader.loadModule("my-module").getDependencies())
        .map(Object::toString).collect(Collectors.toList()))
            .containsExactlyInAnyOrder("dependency on java.base");
    geodeModuleLoader.loadModule("other-module");
    geodeModuleLoader.linkModules("my-module", "other-module", true);
    assertThat(Arrays.stream(geodeModuleLoader.loadModule("my-module").getDependencies())
        .map(Object::toString).collect(Collectors.toList()))
            .containsExactlyInAnyOrder("dependency on java.base", "dependency on other-module");
  }

  @Test
  public void testRegisterModuleAsDependencyOfNullModule() {
    assertThatThrownBy(
        () -> geodeModuleLoader.linkModules(null, "other-module", true))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Module name cannot be null");
  }

  @Test
  public void testRegisterNullModuleAsDependencyOfModule() {
    assertThatThrownBy(
        () -> geodeModuleLoader.linkModules("my-module", null, true))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Module to depend on cannot be null");
  }

  @Test
  public void testUnregisterAddedModuleDependencyFromModules() throws ModuleLoadException {
    addModuleToFinder("other-module");
    addModuleToFinder("my-module");
    geodeModuleLoader.loadModule("my-module");
    geodeModuleLoader.linkModules("my-module", "other-module", true);
    assertThat(Arrays.stream(geodeModuleLoader.loadModule("my-module").getDependencies())
        .map(Object::toString).collect(Collectors.toList()))
            .containsExactlyInAnyOrder("dependency on java.base",
                "dependency on other-module");
    geodeModuleLoader.unregisterModuleDependencyFromModules("other-module");
    assertThat(Arrays.stream(geodeModuleLoader.loadModule("my-module").getDependencies())
        .map(Object::toString).collect(Collectors.toList()))
            .containsExactlyInAnyOrder("dependency on java.base");
    assertThat(geodeModuleLoader.loadModule("other-module")).isNotNull();
  }

  @Test
  public void testUnregisterModuleDependencyFromModules() throws ModuleLoadException {
    addModuleToFinder("other-module");
    addModuleToFinder("my-module", "other-module");
    assertThat(Arrays.stream(geodeModuleLoader.loadModule("my-module").getDependencies())
        .map(Object::toString).collect(Collectors.toList()))
            .containsExactlyInAnyOrder("dependency on java.base",
                "dependency on other-module");
    geodeModuleLoader.unregisterModuleDependencyFromModules("other-module");
    assertThat(Arrays.stream(geodeModuleLoader.loadModule("my-module").getDependencies())
        .map(Object::toString).collect(Collectors.toList()))
            .containsExactlyInAnyOrder("dependency on java.base");
    assertThat(geodeModuleLoader.loadModule("other-module")).isNotNull();
  }

  @Test
  public void testUnregisterInvalidModuleDependencyFromModules() throws ModuleLoadException {
    addModuleToFinder("my-module");
    assertThat(Arrays.stream(geodeModuleLoader.loadModule("my-module").getDependencies())
        .map(Object::toString).collect(Collectors.toList()))
            .containsExactlyInAnyOrder("dependency on java.base");
    geodeModuleLoader.unregisterModuleDependencyFromModules("wrong-module");
    assertThat(Arrays.stream(geodeModuleLoader.loadModule("my-module").getDependencies())
        .map(Object::toString).collect(Collectors.toList()))
            .containsExactlyInAnyOrder("dependency on java.base");
  }

  @Test
  public void testUnregisterNullModuleDependencyFromModules() {
    assertThatThrownBy(() -> geodeModuleLoader.unregisterModuleDependencyFromModules(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Module dependency name cannot be null");
  }

  private void addModuleToFinder(String value, String... dependencies) {
    ModuleSpec.Builder builder = ModuleSpec.build(value);
    for (String dependency : dependencies) {
      builder.addDependency(new ModuleDependencySpecBuilder().setName(dependency).build());
    }
    delegatingModuleFinder.addModuleSpec(builder.create());
  }
}
