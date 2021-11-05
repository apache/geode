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
import java.util.stream.Collectors;

import org.junit.Test;

public class ModuleSpecUtilsTest {
  @Test
  public void testCreateBuilderWithJavaBase() {
    ModuleSpec.Builder builder = ModuleSpecUtils.createBuilder("my-module", true);
    ModuleSpec moduleSpec = builder.create();
    assertThat(moduleSpec.getName()).isEqualTo("my-module");
    assertThat(((ConcreteModuleSpec) moduleSpec).getDependencies().length).isEqualTo(1);
  }

  @Test
  public void testCreateBuilderWithoutJavaBase() {
    ModuleSpec.Builder builder = ModuleSpecUtils.createBuilder("my-module", false);
    ModuleSpec moduleSpec = builder.create();
    assertThat(moduleSpec.getName()).isEqualTo("my-module");
    assertThat(((ConcreteModuleSpec) moduleSpec).getDependencies()).isEmpty();
  }

  @Test
  public void testCreateBuilderWithNullName() {
    assertThatThrownBy(() -> ModuleSpecUtils.createBuilder(null, true))
        .hasMessageContaining("name is null");
  }

  @Test
  public void addModuleDependencyToSpec() {
    ModuleSpec.Builder builder = ModuleSpecUtils.createBuilder("my-module", true);
    ModuleSpec moduleSpec =
        ModuleSpecUtils.addModuleDependencyToSpec(builder.create(), "my-dependency");

    assertThat(moduleSpec.getName()).isEqualTo("my-module");
    assertThat(((ConcreteModuleSpec) moduleSpec).getDependencies().length).isEqualTo(2);
    assertThat(Arrays.stream(((ConcreteModuleSpec) moduleSpec).getDependencies())
        .map(it -> ((ModuleDependencySpec) it).getName()).toArray()).contains("my-dependency");
  }

  @Test
  public void addModuleDependencyToNulSpec() {
    assertThatThrownBy(() -> ModuleSpecUtils.addModuleDependencyToSpec(null, "my-dependency"))
        .hasMessageContaining("ModuleSpec cannot be null");
  }

  @Test
  public void addNullModuleDependencyToSpec() {
    ModuleSpec.Builder builder = ModuleSpecUtils.createBuilder("my-module", true);
    assertThatThrownBy(
        () -> ModuleSpecUtils.addModuleDependencyToSpec(builder.create(), (String[]) null))
            .hasMessageContaining("Dependency names cannot be null");
  }

  @Test
  public void testAddSystemClasspathDependency() {
    ModuleSpec moduleSpec = ModuleSpecUtils
        .addSystemClasspathDependency(ModuleSpecUtils.createBuilder("my-module", true).create());

    assertThat(moduleSpec.getName()).isEqualTo("my-module");

    assertThat(Arrays.stream(((ConcreteModuleSpec) moduleSpec).getDependencies())
        .map(Object::toString).collect(Collectors.toList()))
            .containsExactlyInAnyOrder("dependency on java.base");
  }

  @Test
  public void testAddSystemClasspathDependencyToNullSpec() {
    assertThat(ModuleSpecUtils.addSystemClasspathDependency(null)).isNull();
  }

  @Test
  public void testRemoveDependencyFromSpec() {
    ModuleSpec.Builder builder = ModuleSpecUtils.createBuilder("my-module", true);
    ModuleSpec moduleSpec =
        ModuleSpecUtils.addModuleDependencyToSpec(builder.create(), "my-dependency");

    assertThat(((ConcreteModuleSpec) moduleSpec).getDependencies().length).isEqualTo(2);
    ModuleSpec moduleSpecAfterRemoval =
        ModuleSpecUtils.removeDependencyFromSpec(moduleSpec, "my-dependency");
    assertThat(((ConcreteModuleSpec) moduleSpecAfterRemoval).getDependencies().length).isEqualTo(1);
  }

  @Test
  public void testRemoveNonexistentDependencyFromSpec() {
    ModuleSpec moduleSpec = ModuleSpecUtils.createBuilder("my-module", true).create();

    assertThat(((ConcreteModuleSpec) moduleSpec).getDependencies().length).isEqualTo(1);
    ModuleSpec moduleSpecAfterRemoval =
        ModuleSpecUtils.removeDependencyFromSpec(moduleSpec, "my-dependency");
    assertThat(((ConcreteModuleSpec) moduleSpecAfterRemoval).getDependencies().length).isEqualTo(1);
  }

  @Test
  public void testRemoveNullDependencyFromSpec() {
    ModuleSpec moduleSpec = ModuleSpecUtils.createBuilder("my-module", true).create();

    assertThat(((ConcreteModuleSpec) moduleSpec).getDependencies().length).isEqualTo(1);
    ModuleSpec moduleSpecAfterRemoval = ModuleSpecUtils.removeDependencyFromSpec(moduleSpec,
        (String) null);
    assertThat(((ConcreteModuleSpec) moduleSpecAfterRemoval).getDependencies().length).isEqualTo(1);
  }

  @Test
  public void testRemoveDependencyFromNullSpec() {
    assertThatThrownBy(() -> ModuleSpecUtils.removeDependencyFromSpec(null, "my-dependencies"))
        .hasMessageContaining("ModuleSpec cannot be null");
  }

  @Test
  public void testRemoveNullDependencyFromNullSpec() {
    assertThatThrownBy(() -> ModuleSpecUtils.removeDependencyFromSpec(null, (String) null))
        .hasMessageContaining("ModuleSpec cannot be null");
  }

  @Test
  public void testRemoveDependencyFromNullSpecWithNoDependenciesGiven() {
    assertThatThrownBy(() -> ModuleSpecUtils.removeDependencyFromSpec(null))
        .hasMessageContaining("ModuleSpec cannot be null");
  }

  @Test
  public void testRemoveDependencyFromSpecWithNoDependenciesGiven() {
    ModuleSpec moduleSpec = ModuleSpecUtils.createBuilder("my-module", true).create();

    ModuleSpec moduleSpecAfterRemoval = ModuleSpecUtils.removeDependencyFromSpec(moduleSpec);
    assertThat(moduleSpecAfterRemoval).isEqualTo(moduleSpec);
  }

  @Test
  public void testModuleExportsModuleDependencyNotDependent() {
    ModuleSpec moduleSpec = ModuleSpecUtils.createBuilder("my-module", true).create();

    Boolean moduleDependentOnModule =
        ModuleSpecUtils.moduleExportsModuleDependency(moduleSpec, "my-dependency");

    assertThat(moduleDependentOnModule).isNull();
  }

  @Test
  public void testModuleExportsModuleDependencyDependent() {
    ModuleSpec.Builder builder = ModuleSpecUtils.createBuilder("my-module", true);

    builder.addDependency(new ModuleDependencySpecBuilder()
        .setName("my-dependency")
        .setExport(false)
        .build());

    Boolean moduleDependentOnModule =
        ModuleSpecUtils.moduleExportsModuleDependency(builder.create(), "my-dependency");

    assertThat(moduleDependentOnModule).isFalse();
  }

  @Test
  public void testModuleExportsModuleDependencyExports() {
    ModuleSpec.Builder builder = ModuleSpecUtils.createBuilder("my-module", true);

    ModuleSpec moduleSpec =
        ModuleSpecUtils.addModuleDependencyToSpec(builder.create(), "my-dependency");
    Boolean moduleDependentOnModule =
        ModuleSpecUtils.moduleExportsModuleDependency(moduleSpec, "my-dependency");

    assertThat(moduleDependentOnModule).isTrue();
  }

  @Test
  public void testModuleExportsNullModuleDependency() {
    ModuleSpec.Builder builder = ModuleSpecUtils.createBuilder("my-module", true);

    Boolean moduleDependentOnModule =
        ModuleSpecUtils.moduleExportsModuleDependency(builder.create(), null);

    assertThat(moduleDependentOnModule).isNull();
  }

  @Test
  public void testModuleExportsModuleDependencyWithNUllSpec() {
    assertThatThrownBy(() -> ModuleSpecUtils.moduleExportsModuleDependency(null, "my-dependency"))
        .hasMessageContaining("ModuleSpec cannot be null");
  }
}
