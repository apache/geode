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
package org.jboss.modules;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Ignore;
import org.junit.Test;

public class ModuleSpecUtilTest {
  @Test
  public void testCreateBuilderWithJavaBase() {
    ModuleSpec.Builder builder = ModuleSpecUtil.createBuilder("my-module", true);
    ModuleSpec moduleSpec = builder.create();
    assertThat(moduleSpec.getName()).isEqualTo("my-module");
    assertThat(((ConcreteModuleSpec) moduleSpec).getDependencies().length).isEqualTo(1);
  }

  @Test
  public void testCreateBuilderWithoutJavaBase() {
    ModuleSpec.Builder builder = ModuleSpecUtil.createBuilder("my-module", false);
    ModuleSpec moduleSpec = builder.create();
    assertThat(moduleSpec.getName()).isEqualTo("my-module");
    assertThat(((ConcreteModuleSpec) moduleSpec).getDependencies()).isEmpty();
  }

  @Test
  public void testCreateBuilderWithNullName() {
    assertThatThrownBy(() -> ModuleSpecUtil.createBuilder(null, true))
        .hasMessageContaining("name is null");
  }

  @Test
  public void addModuleDependencyToSpec() {
    ModuleSpec.Builder builder = ModuleSpecUtil.createBuilder("my-module", true);
    ModuleSpec moduleSpec =
        ModuleSpecUtil.addModuleDependencyToSpec(builder.create(), "my-dependency");

    assertThat(moduleSpec.getName()).isEqualTo("my-module");
    assertThat(((ConcreteModuleSpec) moduleSpec).getDependencies().length).isEqualTo(2);
    assertThat(Arrays.stream(((ConcreteModuleSpec) moduleSpec).getDependencies())
        .map(it -> ((ModuleDependencySpec) it).getName()).toArray()).contains("my-dependency");
  }

  @Test
  public void addModuleDependencyToNulSpec() {
    assertThatThrownBy(() -> ModuleSpecUtil.addModuleDependencyToSpec(null, "my-dependency"))
        .hasMessageContaining("ModuleSpec cannot be null");
  }

  @Test
  public void addNullModuleDependencyToSpec() {
    ModuleSpec.Builder builder = ModuleSpecUtil.createBuilder("my-module", true);
    assertThatThrownBy(() -> ModuleSpecUtil.addModuleDependencyToSpec(builder.create(), null))
        .hasMessageContaining("name is null");
  }

  @Test
  @Ignore
  public void testAddSystemClasspathDependency() {
    ModuleSpec moduleSpec = ModuleSpecUtil
        .addSystemClasspathDependency(ModuleSpecUtil.createBuilder("my-module", true).create());

    assertThat(moduleSpec.getName()).isEqualTo("my-module");

    assertThat(((ConcreteModuleSpec) moduleSpec).getDependencies().length).isEqualTo(2);
  }

  @Test
  public void testAddSystemClasspathDependencyToNullSpec() {
    assertThat(ModuleSpecUtil.addSystemClasspathDependency(null)).isNull();
  }

  @Test
  public void testRemoveDependencyFromSpec() {
    ModuleSpec.Builder builder = ModuleSpecUtil.createBuilder("my-module", true);
    ModuleSpec moduleSpec =
        ModuleSpecUtil.addModuleDependencyToSpec(builder.create(), "my-dependency");

    assertThat(((ConcreteModuleSpec) moduleSpec).getDependencies().length).isEqualTo(2);
    ModuleSpec moduleSpecAfterRemoval =
        ModuleSpecUtil.removeDependencyFromSpec(moduleSpec, "my-dependency");
    assertThat(((ConcreteModuleSpec) moduleSpecAfterRemoval).getDependencies().length).isEqualTo(1);
  }

  @Test
  public void testRemoveNonexistentDependencyFromSpec() {
    ModuleSpec moduleSpec = ModuleSpecUtil.createBuilder("my-module", true).create();

    assertThat(((ConcreteModuleSpec) moduleSpec).getDependencies().length).isEqualTo(1);
    ModuleSpec moduleSpecAfterRemoval =
        ModuleSpecUtil.removeDependencyFromSpec(moduleSpec, "my-dependency");
    assertThat(((ConcreteModuleSpec) moduleSpecAfterRemoval).getDependencies().length).isEqualTo(1);
  }

  @Test
  public void testRemoveNullDependencyFromSpec() {
    ModuleSpec moduleSpec = ModuleSpecUtil.createBuilder("my-module", true).create();

    assertThat(((ConcreteModuleSpec) moduleSpec).getDependencies().length).isEqualTo(1);
    ModuleSpec moduleSpecAfterRemoval = ModuleSpecUtil.removeDependencyFromSpec(moduleSpec,
        (String) null);
    assertThat(((ConcreteModuleSpec) moduleSpecAfterRemoval).getDependencies().length).isEqualTo(1);
  }

  @Test
  public void testRemoveDependencyFromNullSpec() {
    assertThatThrownBy(() -> ModuleSpecUtil.removeDependencyFromSpec(null, "my-dependencies"))
        .hasMessageContaining("ModuleSpec cannot be null");
  }

  @Test
  public void testRemoveNullDependencyFromNullSpec() {
    assertThatThrownBy(() -> ModuleSpecUtil.removeDependencyFromSpec(null, (String) null))
        .hasMessageContaining("ModuleSpec cannot be null");
  }

  @Test
  public void testRemoveDependencyFromNullSpecWithNoDependenciesGiven() {
    assertThatThrownBy(() -> ModuleSpecUtil.removeDependencyFromSpec(null))
        .hasMessageContaining("ModuleSpec cannot be null");
  }

  @Test
  public void testRemoveDependencyFromSpecWithNoDependenciesGiven() {
    ModuleSpec moduleSpec = ModuleSpecUtil.createBuilder("my-module", true).create();

    ModuleSpec moduleSpecAfterRemoval = ModuleSpecUtil.removeDependencyFromSpec(moduleSpec);
    assertThat(moduleSpecAfterRemoval).isEqualTo(moduleSpec);
  }

  @Test
  public void testModuleExportsModuleDependencyNotDependent() {
    ModuleSpec moduleSpec = ModuleSpecUtil.createBuilder("my-module", true).create();

    Boolean moduleDependentOnModule =
        ModuleSpecUtil.moduleExportsModuleDependency(moduleSpec, "my-dependency");

    assertThat(moduleDependentOnModule).isNull();
  }

  @Test
  public void testModuleExportsModuleDependencyDependent() {
    ModuleSpec.Builder builder = ModuleSpecUtil.createBuilder("my-module", true);

    builder.addDependency(new ModuleDependencySpecBuilder()
        .setName("my-dependency")
        .setExport(false)
        .build());

    Boolean moduleDependentOnModule =
        ModuleSpecUtil.moduleExportsModuleDependency(builder.create(), "my-dependency");

    assertThat(moduleDependentOnModule).isFalse();
  }

  @Test
  public void testModuleExportsModuleDependencyExports() {
    ModuleSpec.Builder builder = ModuleSpecUtil.createBuilder("my-module", true);

    ModuleSpec moduleSpec =
        ModuleSpecUtil.addModuleDependencyToSpec(builder.create(), "my-dependency");
    Boolean moduleDependentOnModule =
        ModuleSpecUtil.moduleExportsModuleDependency(moduleSpec, "my-dependency");

    assertThat(moduleDependentOnModule).isTrue();
  }

  @Test
  public void testModuleExportsNullModuleDependency() {
    ModuleSpec.Builder builder = ModuleSpecUtil.createBuilder("my-module", true);

    Boolean moduleDependentOnModule =
        ModuleSpecUtil.moduleExportsModuleDependency(builder.create(), null);

    assertThat(moduleDependentOnModule).isNull();
  }

  @Test
  public void testModuleExportsModuleDependencyWithNUllSpec() {
    assertThatThrownBy(() -> ModuleSpecUtil.moduleExportsModuleDependency(null, "my-dependency"))
        .hasMessageContaining("ModuleSpec cannot be null");
  }

  @Test
  public void testAddExcludeFilter() {
    List<String> pathsToExclude = new LinkedList<>();
    pathsToExclude.add("path/to/exclude");
    List<String> pathsToExcludeChildrenOf = new LinkedList<>();
    pathsToExcludeChildrenOf.add("child/path");

    ModuleSpec.Builder builder = ModuleSpecUtil.createBuilder("my-module", true);
    ModuleSpec moduleSpec =
        ModuleSpecUtil.addModuleDependencyToSpec(builder.create(), "my-dependency");
    ModuleSpec moduleSpecWithExclude =
        ModuleSpecUtil.addExcludeFilter(moduleSpec, pathsToExclude, pathsToExcludeChildrenOf,
            "my-dependency");

    ConcreteModuleSpec concreteModuleSpec = (ConcreteModuleSpec) moduleSpecWithExclude;
    assertThat(concreteModuleSpec.getDependencies().length).isEqualTo(2);

    List<ModuleDependencySpec> dependencySpecs =
        Arrays.stream(concreteModuleSpec.getDependencies()).map(it -> (ModuleDependencySpec) it)
            .filter(it -> it.getName().equals("my-dependency")).collect(Collectors.toList());

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
  public void testAddExcludeFilterOnNonexistentModuleDependency() {
    List<String> pathsToExclude = new LinkedList<>();
    pathsToExclude.add("path/to/exclude");
    List<String> pathsToExcludeChildrenOf = new LinkedList<>();

    ModuleSpec.Builder builder = ModuleSpecUtil.createBuilder("my-module", true);
    ModuleSpec moduleSpec = builder.create();
    ModuleSpec moduleSpecWithExclude =
        ModuleSpecUtil.addExcludeFilter(moduleSpec, pathsToExclude, pathsToExcludeChildrenOf,
            "my-dependency");

    assertThat(moduleSpecWithExclude).isEqualTo(moduleSpec);
  }

  @Test
  public void testAddExcludeFilterToNullModuleSpec() {
    List<String> pathsToExclude = new LinkedList<>();
    List<String> pathsToExcludeChildrenOf = new LinkedList<>();

    assertThatThrownBy(
        () -> ModuleSpecUtil.addExcludeFilter(null, pathsToExclude, pathsToExcludeChildrenOf,
            "my-dependency")).hasMessageContaining("ModuleSpec cannot be null");
  }

  @Test
  public void testAddExcludeFilterOnNullModule() {
    List<String> pathsToExclude = new LinkedList<>();
    List<String> pathsToExcludeChildrenOf = new LinkedList<>();

    ModuleSpec.Builder builder = ModuleSpecUtil.createBuilder("my-module", true);
    assertThatThrownBy(() -> ModuleSpecUtil.addExcludeFilter(builder.create(), pathsToExclude,
        pathsToExcludeChildrenOf,
        null)).hasMessageContaining("Module to exclude from cannot be null");
  }

  @Test
  public void testAddExcludeFilterWithNullPathsToExclude() {
    List<String> pathsToExcludeChildrenOf = new LinkedList<>();
    pathsToExcludeChildrenOf.add("child/path");

    ModuleSpec.Builder builder = ModuleSpecUtil.createBuilder("my-module", true);
    ModuleSpec moduleSpec =
        ModuleSpecUtil.addModuleDependencyToSpec(builder.create(), "my-dependency");
    ModuleSpec moduleSpecWithExclude =
        ModuleSpecUtil.addExcludeFilter(moduleSpec, null, pathsToExcludeChildrenOf,
            "my-dependency");

    ConcreteModuleSpec concreteModuleSpec = (ConcreteModuleSpec) moduleSpecWithExclude;
    assertThat(concreteModuleSpec.getDependencies().length).isEqualTo(2);

    List<ModuleDependencySpec> dependencySpecs =
        Arrays.stream(concreteModuleSpec.getDependencies()).map(it -> (ModuleDependencySpec) it)
            .filter(it -> it.getName().equals("my-dependency")).collect(Collectors.toList());

    assertThat(dependencySpecs.size()).isEqualTo(1);

    ModuleDependencySpec moduleDependencySpec = dependencySpecs.get(0);
    assertThat(moduleDependencySpec.getImportFilter().accept("child/path/exclude")).isFalse();
    assertThat(moduleDependencySpec.getImportFilter().accept("path/to/accept")).isTrue();
    assertThat(moduleDependencySpec.getExportFilter().accept("child/path/exclude")).isFalse();
    assertThat(moduleDependencySpec.getExportFilter().accept("path/to/accept")).isTrue();
  }

  @Test
  public void testAddExcludeFilterWithNullChildPaths() {
    List<String> pathsToExclude = new LinkedList<>();
    pathsToExclude.add("path/to/exclude");

    ModuleSpec.Builder builder = ModuleSpecUtil.createBuilder("my-module", true);
    ModuleSpec moduleSpec =
        ModuleSpecUtil.addModuleDependencyToSpec(builder.create(), "my-dependency");
    ModuleSpec moduleSpecWithExclude =
        ModuleSpecUtil.addExcludeFilter(moduleSpec, pathsToExclude, null, "my-dependency");

    ConcreteModuleSpec concreteModuleSpec = (ConcreteModuleSpec) moduleSpecWithExclude;
    assertThat(concreteModuleSpec.getDependencies().length).isEqualTo(2);

    List<ModuleDependencySpec> dependencySpecs =
        Arrays.stream(concreteModuleSpec.getDependencies()).map(it -> (ModuleDependencySpec) it)
            .filter(it -> it.getName().equals("my-dependency")).collect(Collectors.toList());

    assertThat(dependencySpecs.size()).isEqualTo(1);

    ModuleDependencySpec moduleDependencySpec = dependencySpecs.get(0);
    assertThat(moduleDependencySpec.getImportFilter().accept("path/to/exclude")).isFalse();
    assertThat(moduleDependencySpec.getImportFilter().accept("path/to/accept")).isTrue();
    assertThat(moduleDependencySpec.getExportFilter().accept("path/to/exclude")).isFalse();
    assertThat(moduleDependencySpec.getExportFilter().accept("path/to/accept")).isTrue();
  }
}
