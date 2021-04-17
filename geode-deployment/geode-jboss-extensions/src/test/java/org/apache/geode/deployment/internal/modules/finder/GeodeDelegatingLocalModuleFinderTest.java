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
import static org.mockito.Mockito.when;

import org.jboss.modules.ConcreteModuleSpec;
import org.jboss.modules.ModuleFinder;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleLoader;
import org.jboss.modules.ModuleSpec;
import org.junit.Before;
import org.junit.Test;

public class GeodeDelegatingLocalModuleFinderTest {

  private ModuleLoader moduleLoader;
  private ModuleFinder moduleFinder;

  @Before
  public void setup() {
    moduleLoader = mock(ModuleLoader.class);
    moduleFinder = mock(ModuleFinder.class);
  }

  @Test
  public void testFindModuleWithMatchingName() throws ModuleLoadException {
    ModuleSpec moduleSpecToReturn = ModuleSpec.build("my-module").create();
    when(moduleFinder.findModule("my-module", moduleLoader)).thenReturn(moduleSpecToReturn);
    GeodeDelegatingLocalModuleFinder geodeDelegatingLocalModuleFinder =
        new GeodeDelegatingLocalModuleFinder(moduleFinder);
    ModuleSpec moduleSpec = geodeDelegatingLocalModuleFinder.findModule("my-module", moduleLoader);
    assertThat(moduleSpec).isNotNull();
    ConcreteModuleSpec concreteModuleSpec = (ConcreteModuleSpec) moduleSpec;
    assertThat(concreteModuleSpec.getName()).isEqualTo("my-module");
    assertThat(concreteModuleSpec.getDependencies().length).isEqualTo(1);
  }

  @Test
  public void testFindModuleForNonMatchingModuleName() throws ModuleLoadException {
    GeodeDelegatingLocalModuleFinder geodeDelegatingLocalModuleFinder =
        new GeodeDelegatingLocalModuleFinder(moduleFinder);
    ModuleSpec moduleSpec =
        geodeDelegatingLocalModuleFinder.findModule("another-module", moduleLoader);
    assertThat(moduleSpec).isNull();
  }

  @Test
  public void testCreateFinderWithNullDelegate() {
    assertThatThrownBy(() -> new GeodeDelegatingLocalModuleFinder(null))
        .hasMessageContaining("ModuleFinder cannot be null");
  }
}
