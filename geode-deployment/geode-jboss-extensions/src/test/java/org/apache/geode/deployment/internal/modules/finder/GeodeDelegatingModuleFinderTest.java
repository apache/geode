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

import java.util.List;

import org.jboss.modules.ConcreteModuleSpec;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleLoader;
import org.jboss.modules.ModuleSpec;
import org.junit.Before;
import org.junit.Test;


public class GeodeDelegatingModuleFinderTest {

  private ModuleLoader moduleLoader;
  private GeodeDelegatingModuleFinder geodeDelegatingModuleFinder;

  @Before
  public void setup() {
    moduleLoader = mock(ModuleLoader.class);
    geodeDelegatingModuleFinder = new GeodeDelegatingModuleFinder();
  }

  @Test
  public void testFindModuleWithNoFindersAdded() throws ModuleLoadException {
    assertThat(geodeDelegatingModuleFinder.findModule("my-module", moduleLoader)).isNull();
  }

  @Test
  public void testAddNUllModuleSpec() {
    assertThatThrownBy(() -> geodeDelegatingModuleFinder.addModuleSpec(null))
        .hasMessageContaining("ModuleSpec cannot be null");
  }

  @Test
  public void testFindModuleWithMatchingSpecAdded() throws ModuleLoadException {
    ModuleSpec moduleSpecToReturn = ModuleSpec.build("my-module").create();
    geodeDelegatingModuleFinder.addModuleSpec(moduleSpecToReturn);
    ModuleSpec moduleSpec = geodeDelegatingModuleFinder.findModule("my-module", moduleLoader);
    assertThat(moduleSpec).isEqualTo(moduleSpecToReturn);
  }

  @Test
  public void testFindModuleWithNonMatchingFinderAddedForName() throws ModuleLoadException {
    ModuleSpec moduleSpecToReturn = ModuleSpec.build("wrong-module").create();
    geodeDelegatingModuleFinder.addModuleSpec(moduleSpecToReturn);
    ModuleSpec moduleSpec = geodeDelegatingModuleFinder.findModule("my-module", moduleLoader);
    assertThat(moduleSpec).isNull();
  }

  @Test
  public void testRemoveModuleSpec() throws ModuleLoadException {
    ModuleSpec moduleSpecToReturn = ModuleSpec.build("my-module").create();
    geodeDelegatingModuleFinder.addModuleSpec(moduleSpecToReturn);
    ModuleSpec moduleSpec = geodeDelegatingModuleFinder.findModule("my-module", moduleLoader);
    assertThat(moduleSpec).isEqualTo(moduleSpecToReturn);
    geodeDelegatingModuleFinder.removeModuleSpec("my-module");
    assertThat(geodeDelegatingModuleFinder.findModule("my-module", moduleLoader)).isNull();
  }

  @Test
  public void testRemoveModuleFinderInvalidName() throws ModuleLoadException {
    ModuleSpec moduleSpecToReturn = ModuleSpec.build("my-module").create();
    geodeDelegatingModuleFinder.addModuleSpec(moduleSpecToReturn);
    ModuleSpec moduleSpec = geodeDelegatingModuleFinder.findModule("my-module", moduleLoader);
    assertThat(moduleSpec).isEqualTo(moduleSpecToReturn);
    geodeDelegatingModuleFinder.removeModuleSpec("wrong-module");
    assertThat(geodeDelegatingModuleFinder.findModule("my-module", moduleLoader))
        .isEqualTo(moduleSpecToReturn);
  }

  @Test
  public void testRemoveModuleFinderForNull() {
    geodeDelegatingModuleFinder.removeModuleSpec(null);
  }

  @Test
  public void testAddDependencyToLoadedModule() throws ModuleLoadException {
    ModuleSpec moduleSpecToReturn = ModuleSpec.build("my-module").create();
    geodeDelegatingModuleFinder.addModuleSpec(moduleSpecToReturn);
    ModuleSpec moduleSpec = geodeDelegatingModuleFinder.findModule("my-module", moduleLoader);
    assertThat(((ConcreteModuleSpec) moduleSpec).getDependencies().length).isEqualTo(1);
    geodeDelegatingModuleFinder.addDependencyToModule("my-module", "other-module", true);
    ModuleSpec moduleSpecAfterAdd =
        geodeDelegatingModuleFinder.findModule("my-module", moduleLoader);
    assertThat(((ConcreteModuleSpec) moduleSpecAfterAdd).getDependencies().length).isEqualTo(2);
  }

  @Test
  public void testAddDependencyToInvalidModule() {
    assertThatThrownBy(
        () -> geodeDelegatingModuleFinder.addDependencyToModule("my-module", "other-module", true))
            .hasMessageContaining("No such module: my-module");
  }

  @Test
  public void testAddDependencyToNullModule() {
    assertThatThrownBy(
        () -> geodeDelegatingModuleFinder.addDependencyToModule(null, "other-module", true))
            .hasMessageContaining("Module name cannot be null");
  }

  @Test
  public void testAddNullDependencyToModule() throws ModuleLoadException {
    ModuleSpec moduleSpecToReturn = ModuleSpec.build("my-module").create();
    geodeDelegatingModuleFinder.addModuleSpec(moduleSpecToReturn);
    assertThatThrownBy(
        () -> geodeDelegatingModuleFinder.addDependencyToModule("my-module", null, true))
            .hasMessageContaining("Module to depend on cannot be null");
  }

  @Test
  public void testRemoveDependencyFromModules() throws ModuleLoadException {
    ModuleSpec moduleSpecToReturn = ModuleSpec.build("my-module").create();
    geodeDelegatingModuleFinder.addModuleSpec(moduleSpecToReturn);
    ModuleSpec moduleSpec = geodeDelegatingModuleFinder.findModule("my-module", moduleLoader);
    assertThat(((ConcreteModuleSpec) moduleSpec).getDependencies().length).isEqualTo(1);
    geodeDelegatingModuleFinder.addDependencyToModule("my-module", "other-module", true);
    ModuleSpec moduleSpecAfterAdd =
        geodeDelegatingModuleFinder.findModule("my-module", moduleLoader);
    assertThat(((ConcreteModuleSpec) moduleSpecAfterAdd).getDependencies().length).isEqualTo(2);
    List<String> previouslyDependentModules =
        geodeDelegatingModuleFinder.removeDependencyFromModules("other-module");
    assertThat(previouslyDependentModules).containsExactly("my-module");
    ModuleSpec moduleSpecAfterRemove =
        geodeDelegatingModuleFinder.findModule("my-module", moduleLoader);
    assertThat(((ConcreteModuleSpec) moduleSpecAfterRemove).getDependencies().length).isEqualTo(1);
  }

  @Test
  public void testRemoveDependencyFromModulesWithMultipleModuleLoaded() throws ModuleLoadException {
    ModuleSpec moduleSpecToReturn = ModuleSpec.build("my-module").create();
    geodeDelegatingModuleFinder.addModuleSpec(moduleSpecToReturn);
    geodeDelegatingModuleFinder.addModuleSpec(ModuleSpec.build("other-module").create());
    geodeDelegatingModuleFinder.findModule("my-module", moduleLoader);
    geodeDelegatingModuleFinder.findModule("other-module", moduleLoader);
    geodeDelegatingModuleFinder.addDependencyToModule("my-module", "my-dependency", true);
    geodeDelegatingModuleFinder.addDependencyToModule("other-module", "my-dependency", true);
    List<String> previouslyDependentModules =
        geodeDelegatingModuleFinder.removeDependencyFromModules("other-module");
    assertThat(previouslyDependentModules).isEmpty();
    previouslyDependentModules =
        geodeDelegatingModuleFinder.removeDependencyFromModules("my-dependency");
    assertThat(previouslyDependentModules).containsExactlyInAnyOrder("my-module", "other-module");
  }

  @Test
  public void testRemoveInvalidDependencyFromModules() throws ModuleLoadException {
    ModuleSpec moduleSpecToReturn = ModuleSpec.build("my-module").create();

    geodeDelegatingModuleFinder.addModuleSpec(moduleSpecToReturn);
    List<String> previouslyDependentModules =
        geodeDelegatingModuleFinder.removeDependencyFromModules("other-module");
    assertThat(previouslyDependentModules).isEmpty();
    ModuleSpec moduleSpecAfterRemove =
        geodeDelegatingModuleFinder.findModule("my-module", moduleLoader);
    assertThat(((ConcreteModuleSpec) moduleSpecAfterRemove).getDependencies().length).isEqualTo(1);
  }

  @Test
  public void testRemoveInvalidDependencyFromModulesWhenDependenciesExist()
      throws ModuleLoadException {
    ModuleSpec moduleSpecToReturn = ModuleSpec.build("my-module").create();

    geodeDelegatingModuleFinder.addModuleSpec(moduleSpecToReturn);
    ModuleSpec moduleSpec = geodeDelegatingModuleFinder.findModule("my-module", moduleLoader);
    assertThat(((ConcreteModuleSpec) moduleSpec).getDependencies().length).isEqualTo(1);
    geodeDelegatingModuleFinder.addDependencyToModule("my-module", "other-module", true);
    ModuleSpec moduleSpecAfterAdd =
        geodeDelegatingModuleFinder.findModule("my-module", moduleLoader);
    assertThat(((ConcreteModuleSpec) moduleSpecAfterAdd).getDependencies().length).isEqualTo(2);
    List<String> previouslyDependentModules =
        geodeDelegatingModuleFinder.removeDependencyFromModules("fake-module");
    assertThat(previouslyDependentModules).isEmpty();
    ModuleSpec moduleSpecAfterRemove =
        geodeDelegatingModuleFinder.findModule("my-module", moduleLoader);
    assertThat(((ConcreteModuleSpec) moduleSpecAfterRemove).getDependencies().length).isEqualTo(2);
  }

  @Test
  public void testRemoveDependencyFromModulesWithNoModuleFindersAdded() throws ModuleLoadException {
    List<String> previouslyDependentModules =
        geodeDelegatingModuleFinder.removeDependencyFromModules("other-module");
    assertThat(previouslyDependentModules).isEmpty();
    ModuleSpec moduleSpecAfterRemove =
        geodeDelegatingModuleFinder.findModule("other-module", moduleLoader);
    assertThat(moduleSpecAfterRemove).isNull();
  }

  @Test
  public void testRemoveDependencyFromModulesForOnlyLoadedModule() throws ModuleLoadException {
    ModuleSpec moduleSpecToReturn = ModuleSpec.build("my-module").create();

    geodeDelegatingModuleFinder.addModuleSpec(moduleSpecToReturn);
    ModuleSpec moduleSpec = geodeDelegatingModuleFinder.findModule("my-module", moduleLoader);
    List<String> previouslyDependentModules =
        geodeDelegatingModuleFinder.removeDependencyFromModules("my-module");
    assertThat(previouslyDependentModules).isEmpty();
    ModuleSpec moduleSpecAfterRemove =
        geodeDelegatingModuleFinder.findModule("my-module", moduleLoader);
    assertThat(moduleSpecAfterRemove).isEqualTo(moduleSpec);
  }

  @Test
  public void testRemoveNullDependencyFromModules() {
    assertThatThrownBy(() -> geodeDelegatingModuleFinder.removeDependencyFromModules(null))
        .hasMessageContaining("Module dependency name cannot be null");
  }
}
