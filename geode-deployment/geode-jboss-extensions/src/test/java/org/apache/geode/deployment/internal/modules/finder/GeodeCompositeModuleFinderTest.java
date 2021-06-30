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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.jboss.modules.ConcreteModuleSpec;
import org.jboss.modules.ModuleDependencySpec;
import org.jboss.modules.ModuleFinder;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleLoader;
import org.jboss.modules.ModuleSpec;
import org.jboss.modules.filter.PathFilter;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.deployment.internal.modules.extensions.GeodeExtension;
import org.apache.geode.deployment.internal.modules.utils.ModuleUtils;

public class GeodeCompositeModuleFinderTest {

  private ModuleLoader moduleLoader;
  private ModuleFinder moduleFinder;
  private GeodeCompositeModuleFinder geodeCompositeModuleFinder;

  @Before
  public void setup() {
    moduleLoader = mock(ModuleLoader.class);
    moduleFinder = mock(ModuleFinder.class);
    geodeCompositeModuleFinder = new GeodeCompositeModuleFinder();
  }

  @Test
  public void testFindModuleWithNoFindersAdded() throws ModuleLoadException {
    assertThat(geodeCompositeModuleFinder.findModule("my-module", moduleLoader)).isNull();
  }

  @Test
  public void testAddNullModuleFinder() {
    assertThatThrownBy(() -> geodeCompositeModuleFinder.addModuleFinder("my-module", null))
        .hasMessageContaining("ModuleFinder cannot be null");
  }

  @Test
  public void testAddModuleFinderForNullName() {
    assertThatThrownBy(() -> geodeCompositeModuleFinder.addModuleFinder(null, moduleFinder))
        .hasMessageContaining("Module name cannot be null");
  }

  @Test
  public void testFindModuleWithMatchingFinderAdded() throws ModuleLoadException {
    ModuleSpec moduleSpecToReturn = ModuleSpec.build("my-module").create();
    when(moduleFinder.findModule(anyString(), any())).thenReturn(moduleSpecToReturn);
    geodeCompositeModuleFinder.addModuleFinder("my-module", moduleFinder);
    ModuleSpec moduleSpec = geodeCompositeModuleFinder.findModule("my-module", moduleLoader);
    assertThat(moduleSpec).isEqualTo(moduleSpecToReturn);
  }

  @Test
  public void testFindModuleWithMatchingFinderAddedForWrongName() throws ModuleLoadException {
    ModuleSpec moduleSpecToReturn = ModuleSpec.build("my-module").create();
    when(moduleFinder.findModule(eq("my-module"), any())).thenReturn(moduleSpecToReturn);
    geodeCompositeModuleFinder.addModuleFinder("wrong-module", moduleFinder);
    ModuleSpec moduleSpec = geodeCompositeModuleFinder.findModule("my-module", moduleLoader);
    assertThat(moduleSpec).isEqualTo(moduleSpecToReturn);
  }

  @Test
  public void testFindModuleWithNonMatchingFinderAddedForName() throws ModuleLoadException {
    ModuleSpec moduleSpecToReturn = ModuleSpec.build("wrong-module").create();
    when(moduleFinder.findModule(eq("wrong-module"), any())).thenReturn(moduleSpecToReturn);
    geodeCompositeModuleFinder.addModuleFinder("my-module", moduleFinder);
    ModuleSpec moduleSpec = geodeCompositeModuleFinder.findModule("my-module", moduleLoader);
    assertThat(moduleSpec).isNull();
  }

  @Test
  public void testRemoveModuleFinder() throws ModuleLoadException {
    ModuleSpec moduleSpecToReturn = ModuleSpec.build("my-module").create();
    when(moduleFinder.findModule(anyString(), any())).thenReturn(moduleSpecToReturn);
    geodeCompositeModuleFinder.addModuleFinder("my-module", moduleFinder);
    ModuleSpec moduleSpec = geodeCompositeModuleFinder.findModule("my-module", moduleLoader);
    assertThat(moduleSpec).isEqualTo(moduleSpecToReturn);
    geodeCompositeModuleFinder.removeModuleFinder("my-module");
    assertThat(geodeCompositeModuleFinder.findModule("my-module", moduleLoader)).isNull();
  }

  @Test
  public void testRemoveModuleFinderInvalidName() throws ModuleLoadException {
    ModuleSpec moduleSpecToReturn = ModuleSpec.build("my-module").create();
    when(moduleFinder.findModule(anyString(), any())).thenReturn(moduleSpecToReturn);
    geodeCompositeModuleFinder.addModuleFinder("my-module", moduleFinder);
    ModuleSpec moduleSpec = geodeCompositeModuleFinder.findModule("my-module", moduleLoader);
    assertThat(moduleSpec).isEqualTo(moduleSpecToReturn);
    geodeCompositeModuleFinder.removeModuleFinder("wrong-module");
    assertThat(geodeCompositeModuleFinder.findModule("my-module", moduleLoader))
        .isEqualTo(moduleSpecToReturn);
  }

  @Test
  public void testRemoveModuleFinderForNull() {
    geodeCompositeModuleFinder.removeModuleFinder(null);
  }

  @Test
  public void testAddDependencyToLoadedModule() throws ModuleLoadException {
    ModuleSpec moduleSpecToReturn = ModuleSpec.build("my-module").create();
    when(moduleFinder.findModule(anyString(), any())).thenReturn(moduleSpecToReturn);
    geodeCompositeModuleFinder.addModuleFinder("my-module", moduleFinder);
    ModuleSpec moduleSpec = geodeCompositeModuleFinder.findModule("my-module", moduleLoader);
    assertThat(((ConcreteModuleSpec) moduleSpec).getDependencies().length).isEqualTo(1);
    geodeCompositeModuleFinder.addDependencyToModule("my-module", "other-module");
    ModuleSpec moduleSpecAfterAdd =
        geodeCompositeModuleFinder.findModule("my-module", moduleLoader);
    assertThat(((ConcreteModuleSpec) moduleSpecAfterAdd).getDependencies().length).isEqualTo(2);
  }

  @Test
  public void testAddDependencyToInvalidModule() {
    assertThatThrownBy(
        () -> geodeCompositeModuleFinder.addDependencyToModule("my-module", "other-module"))
            .hasMessageContaining("No such module: my-module");
  }

  @Test
  public void testAddDependencyToNullModule() {
    assertThatThrownBy(
        () -> geodeCompositeModuleFinder.addDependencyToModule(null, "other-module"))
            .hasMessageContaining("Module name cannot be null");
  }

  @Test
  public void testAddNullDependencyToModule() throws ModuleLoadException {
    ModuleSpec moduleSpecToReturn = ModuleSpec.build("my-module").create();
    when(moduleFinder.findModule(anyString(), any())).thenReturn(moduleSpecToReturn);
    geodeCompositeModuleFinder.addModuleFinder("my-module", moduleFinder);
    assertThatThrownBy(() -> geodeCompositeModuleFinder.addDependencyToModule("my-module", null))
        .hasMessageContaining("Modules to depend on cannot be null");
  }

  @Test
  public void testAddDependencyToUnloadedModule() throws ModuleLoadException {
    ModuleSpec moduleSpecToReturn = ModuleSpec.build("my-module").create();
    when(moduleFinder.findModule(anyString(), any())).thenReturn(moduleSpecToReturn);
    geodeCompositeModuleFinder.addModuleFinder("my-module", moduleFinder);
    assertThatThrownBy(
        () -> geodeCompositeModuleFinder.addDependencyToModule("my-module", "other-module"))
            .hasMessageContaining("No such module: my-module");
  }

  @Test
  public void testRemoveDependencyFromModules() throws ModuleLoadException {
    ModuleSpec moduleSpecToReturn = ModuleSpec.build("my-module").create();
    when(moduleFinder.findModule(anyString(), any())).thenReturn(moduleSpecToReturn);
    geodeCompositeModuleFinder.addModuleFinder("my-module", moduleFinder);
    ModuleSpec moduleSpec = geodeCompositeModuleFinder.findModule("my-module", moduleLoader);
    assertThat(((ConcreteModuleSpec) moduleSpec).getDependencies().length).isEqualTo(1);
    geodeCompositeModuleFinder.addDependencyToModule("my-module", "other-module");
    ModuleSpec moduleSpecAfterAdd =
        geodeCompositeModuleFinder.findModule("my-module", moduleLoader);
    assertThat(((ConcreteModuleSpec) moduleSpecAfterAdd).getDependencies().length).isEqualTo(2);
    List<String> previouslyDependentModules =
        geodeCompositeModuleFinder.removeDependencyFromModules("other-module");
    assertThat(previouslyDependentModules).containsExactly("my-module");
    ModuleSpec moduleSpecAfterRemove =
        geodeCompositeModuleFinder.findModule("my-module", moduleLoader);
    assertThat(((ConcreteModuleSpec) moduleSpecAfterRemove).getDependencies().length).isEqualTo(1);
  }

  @Test
  public void testRemoveDependencyFromModulesWithMultipleModuleLoaded() throws ModuleLoadException {
    ModuleSpec moduleSpecToReturn = ModuleSpec.build("my-module").create();
    when(moduleFinder.findModule(eq("my-module"), any())).thenReturn(moduleSpecToReturn);
    geodeCompositeModuleFinder.addModuleFinder("my-module", moduleFinder);
    ModuleFinder otherModuleFinder = mock(ModuleFinder.class);
    when(otherModuleFinder.findModule(eq("other-module"), any()))
        .thenReturn(ModuleSpec.build("other-module").create());
    geodeCompositeModuleFinder.addModuleFinder("other-module", otherModuleFinder);
    geodeCompositeModuleFinder.findModule("my-module", moduleLoader);
    geodeCompositeModuleFinder.findModule("other-module", moduleLoader);
    geodeCompositeModuleFinder.addDependencyToModule("my-module", "my-dependency");
    geodeCompositeModuleFinder.addDependencyToModule("other-module", "my-dependency");
    List<String> previouslyDependentModules =
        geodeCompositeModuleFinder.removeDependencyFromModules("other-module");
    assertThat(previouslyDependentModules).isEmpty();
    previouslyDependentModules =
        geodeCompositeModuleFinder.removeDependencyFromModules("my-dependency");
    assertThat(previouslyDependentModules).containsExactlyInAnyOrder("my-module", "other-module");
  }

  @Test
  public void testRemoveInvalidDependencyFromModules() throws ModuleLoadException {
    ModuleSpec moduleSpecToReturn = ModuleSpec.build("my-module").create();
    when(moduleFinder.findModule(anyString(), any())).thenReturn(moduleSpecToReturn);
    geodeCompositeModuleFinder.addModuleFinder("my-module", moduleFinder);
    List<String> previouslyDependentModules =
        geodeCompositeModuleFinder.removeDependencyFromModules("other-module");
    assertThat(previouslyDependentModules).isEmpty();
    ModuleSpec moduleSpecAfterRemove =
        geodeCompositeModuleFinder.findModule("my-module", moduleLoader);
    assertThat(((ConcreteModuleSpec) moduleSpecAfterRemove).getDependencies().length).isEqualTo(1);
  }

  @Test
  public void testRemoveInvalidDependencyFromModulesWhenDependenciesExist()
      throws ModuleLoadException {
    ModuleSpec moduleSpecToReturn = ModuleSpec.build("my-module").create();
    when(moduleFinder.findModule(anyString(), any())).thenReturn(moduleSpecToReturn);
    geodeCompositeModuleFinder.addModuleFinder("my-module", moduleFinder);
    ModuleSpec moduleSpec = geodeCompositeModuleFinder.findModule("my-module", moduleLoader);
    assertThat(((ConcreteModuleSpec) moduleSpec).getDependencies().length).isEqualTo(1);
    geodeCompositeModuleFinder.addDependencyToModule("my-module", "other-module");
    ModuleSpec moduleSpecAfterAdd =
        geodeCompositeModuleFinder.findModule("my-module", moduleLoader);
    assertThat(((ConcreteModuleSpec) moduleSpecAfterAdd).getDependencies().length).isEqualTo(2);
    List<String> previouslyDependentModules =
        geodeCompositeModuleFinder.removeDependencyFromModules("fake-module");
    assertThat(previouslyDependentModules).isEmpty();
    ModuleSpec moduleSpecAfterRemove =
        geodeCompositeModuleFinder.findModule("my-module", moduleLoader);
    assertThat(((ConcreteModuleSpec) moduleSpecAfterRemove).getDependencies().length).isEqualTo(2);
  }

  @Test
  public void testRemoveDependencyFromModulesWithNoModuleFindersAdded() throws ModuleLoadException {
    List<String> previouslyDependentModules =
        geodeCompositeModuleFinder.removeDependencyFromModules("other-module");
    assertThat(previouslyDependentModules).isEmpty();
    ModuleSpec moduleSpecAfterRemove =
        geodeCompositeModuleFinder.findModule("other-module", moduleLoader);
    assertThat(moduleSpecAfterRemove).isNull();
  }

  @Test
  public void testRemoveDependencyFromModulesForOnlyLoadedModule() throws ModuleLoadException {
    ModuleSpec moduleSpecToReturn = ModuleSpec.build("my-module").create();
    when(moduleFinder.findModule(anyString(), any())).thenReturn(moduleSpecToReturn);
    geodeCompositeModuleFinder.addModuleFinder("my-module", moduleFinder);
    ModuleSpec moduleSpec = geodeCompositeModuleFinder.findModule("my-module", moduleLoader);
    List<String> previouslyDependentModules =
        geodeCompositeModuleFinder.removeDependencyFromModules("my-module");
    assertThat(previouslyDependentModules).isEmpty();
    ModuleSpec moduleSpecAfterRemove =
        geodeCompositeModuleFinder.findModule("my-module", moduleLoader);
    assertThat(moduleSpecAfterRemove).isEqualTo(moduleSpec);
  }

  @Test
  public void testRemoveNullDependencyFromModules() {
    assertThatThrownBy(() -> geodeCompositeModuleFinder.removeDependencyFromModules(null))
        .hasMessageContaining("Module dependency name cannot be null");
  }

  @Test
  public void testAddExcludeFilterToModule() throws ModuleLoadException {
    List<String> pathsToExclude = new LinkedList<>();
    pathsToExclude.add("path/to/exclude");
    List<String> pathsToExcludeChildrenOf = new LinkedList<>();
    pathsToExcludeChildrenOf.add("child/path");

    ModuleSpec moduleSpecToReturn = ModuleSpec.build("my-module").create();
    when(moduleFinder.findModule(anyString(), any())).thenReturn(moduleSpecToReturn);
    geodeCompositeModuleFinder.addModuleFinder("my-module", moduleFinder);
    geodeCompositeModuleFinder.findModule("my-module", moduleLoader);
    geodeCompositeModuleFinder.addDependencyToModule("my-module", "other-module");

    PathFilter pathFilter =
        ModuleUtils.createPathFilter(pathsToExclude, pathsToExcludeChildrenOf);

    GeodeExtension extension =
        new GeodeExtension("other-module", pathFilter, Collections.emptyList());

    geodeCompositeModuleFinder.addExcludeFilterToModule("my-module", extension);
    ModuleSpec moduleSpecWithExclude =
        geodeCompositeModuleFinder.findModule("my-module", moduleLoader);

    ConcreteModuleSpec concreteModuleSpec = (ConcreteModuleSpec) moduleSpecWithExclude;

    List<ModuleDependencySpec> dependencySpecs =
        Arrays.stream(concreteModuleSpec.getDependencies()).map(it -> (ModuleDependencySpec) it)
            .filter(it -> it.getName().equals("other-module")).collect(Collectors.toList());

    assertThat(dependencySpecs.size()).isEqualTo(1);

    ModuleDependencySpec moduleDependencySpec = dependencySpecs.get(0);
    assertThat(moduleDependencySpec.getImportFilter().accept("path/to/exclude")).isFalse();
    assertThat(moduleDependencySpec.getImportFilter().accept("child/path/exclude")).isFalse();
    assertThat(moduleDependencySpec.getImportFilter().accept("path/to/accept")).isTrue();
    assertThat(moduleDependencySpec.getExportFilter().accept("path/to/exclude")).isFalse();
    assertThat(moduleDependencySpec.getExportFilter().accept("child/path/exclude")).isFalse();
    assertThat(moduleDependencySpec.getExportFilter().accept("path/to/accept")).isTrue();
  }

  @Test
  public void testAddExcludeFilterOnNonexistentModuleDependency() throws ModuleLoadException {
    List<String> pathsToExclude = new LinkedList<>();
    pathsToExclude.add("path/to/exclude");
    List<String> pathsToExcludeChildrenOf = new LinkedList<>();
    pathsToExcludeChildrenOf.add("child/path");

    ModuleSpec moduleSpecToReturn = ModuleSpec.build("my-module").create();
    when(moduleFinder.findModule(anyString(), any())).thenReturn(moduleSpecToReturn);
    geodeCompositeModuleFinder.addModuleFinder("my-module", moduleFinder);
    ModuleSpec moduleSpec = geodeCompositeModuleFinder.findModule("my-module", moduleLoader);

    PathFilter pathFilter =
        ModuleUtils.createPathFilter(pathsToExclude, pathsToExcludeChildrenOf);

    GeodeExtension extension =
        new GeodeExtension("other-module", pathFilter, Collections.emptyList());
    geodeCompositeModuleFinder.addExcludeFilterToModule("my-module", extension);
    ModuleSpec moduleSpecWithExclude =
        geodeCompositeModuleFinder.findModule("my-module", moduleLoader);

    assertThat(moduleSpecWithExclude).isEqualTo(moduleSpec);
  }

  @Test
  public void testAddExcludeFilterWithNullModuleName() throws ModuleLoadException {
    List<String> pathsToExclude = new LinkedList<>();
    pathsToExclude.add("path/to/exclude");
    List<String> pathsToExcludeChildrenOf = new LinkedList<>();
    pathsToExcludeChildrenOf.add("child/path");

    PathFilter pathFilter =
        ModuleUtils.createPathFilter(pathsToExclude, pathsToExcludeChildrenOf);

    GeodeExtension extension =
        new GeodeExtension("other-module", pathFilter, Collections.emptyList());

    assertThatThrownBy(() -> geodeCompositeModuleFinder
        .addExcludeFilterToModule(null, extension))
            .hasMessageContaining("Module name cannot be null");
  }

  @Test
  public void testAddExcludeFilterOnNullModule() throws ModuleLoadException {
    ModuleSpec moduleSpecToReturn = ModuleSpec.build("my-module").create();
    when(moduleFinder.findModule(anyString(), any())).thenReturn(moduleSpecToReturn);
    geodeCompositeModuleFinder.addModuleFinder("my-module", moduleFinder);
    geodeCompositeModuleFinder.findModule("my-module", moduleLoader);

    assertThatThrownBy(
        () -> geodeCompositeModuleFinder.addExcludeFilterToModule("my-module", null))
            .hasMessageContaining("Extensions to exclude from cannot be null");
  }

  @Test
  public void testAddExcludeFilterToUnloadedModule() throws ModuleLoadException {
    List<String> pathsToExclude = new LinkedList<>();
    pathsToExclude.add("path/to/exclude");
    List<String> pathsToExcludeChildrenOf = new LinkedList<>();
    pathsToExcludeChildrenOf.add("child/path");

    ModuleSpec moduleSpecToReturn = ModuleSpec.build("my-module").create();
    when(moduleFinder.findModule(anyString(), any())).thenReturn(moduleSpecToReturn);
    geodeCompositeModuleFinder.addModuleFinder("my-module", moduleFinder);

    PathFilter pathFilter =
        ModuleUtils.createPathFilter(pathsToExclude, pathsToExcludeChildrenOf);

    GeodeExtension extension =
        new GeodeExtension("other-module", pathFilter, Collections.emptyList());

    assertThatThrownBy(() -> geodeCompositeModuleFinder
        .addExcludeFilterToModule("my-module", extension))
            .hasMessageContaining("No such module: my-module");
  }


  @Test
  public void testAddExcludeFilterWithNullPathsToExclude() throws ModuleLoadException {
    List<String> pathsToExcludeChildrenOf = new LinkedList<>();
    pathsToExcludeChildrenOf.add("child/path");

    ModuleSpec moduleSpecToReturn = ModuleSpec.build("my-module").create();
    when(moduleFinder.findModule(anyString(), any())).thenReturn(moduleSpecToReturn);
    geodeCompositeModuleFinder.addModuleFinder("my-module", moduleFinder);
    geodeCompositeModuleFinder.findModule("my-module", moduleLoader);
    geodeCompositeModuleFinder.addDependencyToModule("my-module", "other-module");

    PathFilter pathFilter = ModuleUtils.createPathFilter(null, pathsToExcludeChildrenOf);

    GeodeExtension extension =
        new GeodeExtension("other-module", pathFilter, Collections.emptyList());

    geodeCompositeModuleFinder.addExcludeFilterToModule("my-module", extension);
    ModuleSpec moduleSpecWithExclude =
        geodeCompositeModuleFinder.findModule("my-module", moduleLoader);

    ConcreteModuleSpec concreteModuleSpec = (ConcreteModuleSpec) moduleSpecWithExclude;

    List<ModuleDependencySpec> dependencySpecs =
        Arrays.stream(concreteModuleSpec.getDependencies()).map(it -> (ModuleDependencySpec) it)
            .filter(it -> it.getName().equals("other-module")).collect(Collectors.toList());

    assertThat(dependencySpecs.size()).isEqualTo(1);

    ModuleDependencySpec moduleDependencySpec = dependencySpecs.get(0);
    assertThat(moduleDependencySpec.getImportFilter().accept("child/path/exclude")).isFalse();
    assertThat(moduleDependencySpec.getImportFilter().accept("path/to/accept")).isTrue();
    assertThat(moduleDependencySpec.getExportFilter().accept("child/path/exclude")).isFalse();
    assertThat(moduleDependencySpec.getExportFilter().accept("path/to/accept")).isTrue();
  }

  @Test
  public void testAddExcludeFilterWithNullChildPaths() throws ModuleLoadException {
    List<String> pathsToExclude = new LinkedList<>();
    pathsToExclude.add("path/to/exclude");

    ModuleSpec moduleSpecToReturn = ModuleSpec.build("my-module").create();
    when(moduleFinder.findModule(anyString(), any())).thenReturn(moduleSpecToReturn);
    geodeCompositeModuleFinder.addModuleFinder("my-module", moduleFinder);
    geodeCompositeModuleFinder.findModule("my-module", moduleLoader);
    geodeCompositeModuleFinder.addDependencyToModule("my-module", "other-module");

    PathFilter pathFilter = ModuleUtils.createPathFilter(pathsToExclude, null);

    GeodeExtension extension =
        new GeodeExtension("other-module", pathFilter, Collections.emptyList());

    geodeCompositeModuleFinder.addExcludeFilterToModule("my-module", extension);
    ModuleSpec moduleSpecWithExclude =
        geodeCompositeModuleFinder.findModule("my-module", moduleLoader);

    ConcreteModuleSpec concreteModuleSpec = (ConcreteModuleSpec) moduleSpecWithExclude;

    List<ModuleDependencySpec> dependencySpecs =
        Arrays.stream(concreteModuleSpec.getDependencies()).map(it -> (ModuleDependencySpec) it)
            .filter(it -> it.getName().equals("other-module")).collect(Collectors.toList());

    assertThat(dependencySpecs.size()).isEqualTo(1);

    ModuleDependencySpec moduleDependencySpec = dependencySpecs.get(0);
    assertThat(moduleDependencySpec.getImportFilter().accept("path/to/exclude")).isFalse();
    assertThat(moduleDependencySpec.getImportFilter().accept("path/to/accept")).isTrue();
    assertThat(moduleDependencySpec.getExportFilter().accept("path/to/exclude")).isFalse();
    assertThat(moduleDependencySpec.getExportFilter().accept("path/to/accept")).isTrue();
  }
}
