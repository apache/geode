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
package org.apache.geode.deployment.internal.modules.utils;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.jboss.modules.ConcreteModuleSpec;
import org.jboss.modules.DependencySpec;
import org.jboss.modules.ModuleSpec;
import org.junit.Test;

public class ModuleSpecBuilderUtilsTest {

  @Test
  public void testAddModuleDependencies() {
    ModuleSpec.Builder myModuleBuilder = ModuleSpec.build("myModule");
    ModuleSpec.Builder builder =
        ModuleSpecBuilderUtils.addModuleDependencies(myModuleBuilder, true, "my-module");

    DependencySpec[] dependencies = ((ConcreteModuleSpec) builder.create()).getDependencies();
    assertThat(dependencies.length).isEqualTo(2);

    List<DependencySpec> dependencySpecs =
        Arrays.stream(dependencies).filter(it -> it.toString().contains("my-module"))
            .collect(Collectors.toList());

    assertThat(dependencySpecs.size()).isEqualTo(1);
  }

  @Test
  public void testAddModuleDependenciesDuplicates() {
    ModuleSpec.Builder myModuleBuilder = ModuleSpec.build("myModule");
    ModuleSpec.Builder builder =
        ModuleSpecBuilderUtils.addModuleDependencies(myModuleBuilder, true, "my-module",
            "my-module");

    DependencySpec[] dependencies = ((ConcreteModuleSpec) builder.create()).getDependencies();
    assertThat(dependencies.length).isEqualTo(2);

    List<DependencySpec> dependencySpecs =
        Arrays.stream(dependencies).filter(it -> it.toString().contains("my-module"))
            .collect(Collectors.toList());

    assertThat(dependencySpecs.size()).isEqualTo(1);
  }

  @Test
  public void testAddModuleDependenciesWithNullBuilder() {
    ModuleSpec.Builder builder =
        ModuleSpecBuilderUtils.addModuleDependencies(null, true, "my-module");

    assertThat(builder).isNull();
  }

  @Test
  public void testAddModuleDependenciesWithNullDependency() {
    ModuleSpec.Builder myModuleBuilder = ModuleSpec.build("myModule");
    ModuleSpec.Builder builder =
        ModuleSpecBuilderUtils.addModuleDependencies(myModuleBuilder, true, (String) null);

    DependencySpec[] dependencies = ((ConcreteModuleSpec) builder.create()).getDependencies();
    assertThat(dependencies.length).isEqualTo(1);
  }

  @Test
  public void testAddModuleDependenciesWithNullAndValidDependency() {
    ModuleSpec.Builder myModuleBuilder = ModuleSpec.build("myModule");
    ModuleSpec.Builder builder =
        ModuleSpecBuilderUtils.addModuleDependencies(myModuleBuilder, true, null, "my-module");

    DependencySpec[] dependencies = ((ConcreteModuleSpec) builder.create()).getDependencies();
    assertThat(dependencies.length).isEqualTo(2);

    List<DependencySpec> dependencySpecs =
        Arrays.stream(dependencies).filter(it -> it.toString().contains("my-module"))
            .collect(Collectors.toList());

    assertThat(dependencySpecs.size()).isEqualTo(1);
  }

  @Test
  public void testAddLocalDependencySpec() {
    ModuleSpec.Builder myModuleBuilder = ModuleSpec.build("myModule");
    ModuleSpec.Builder builder =
        ModuleSpecBuilderUtils.addLocalDependencySpec(myModuleBuilder);

    DependencySpec[] dependencies = ((ConcreteModuleSpec) builder.create()).getDependencies();
    assertThat(dependencies.length).isEqualTo(2);

    List<DependencySpec> dependencySpecs =
        Arrays.stream(dependencies).filter(it -> it.toString().contains("local"))
            .collect(Collectors.toList());

    assertThat(dependencySpecs.size()).isEqualTo(1);
  }

  @Test
  public void testAddLocalDependencySpecWithNullBuilder() {
    ModuleSpec.Builder builder =
        ModuleSpecBuilderUtils.addLocalDependencySpec(null);

    assertThat(builder).isNull();
  }
}
