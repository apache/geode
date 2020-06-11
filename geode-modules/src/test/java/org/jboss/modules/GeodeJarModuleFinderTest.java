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

package org.jboss.modules;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.services.module.ModuleDescriptor;

public class GeodeJarModuleFinderTest {

  private static String gemFireVersion = GemFireVersion.getGemFireVersion();

  private static final String MODULE1_PATH =
      System.getProperty("user.dir") + "/../libs/module1WithManifest-1.0.jar";
  private static final String MODULE2_PATH =
      System.getProperty("user.dir") + "/../libs/module2WithManifest-1.0.jar";
  private static final String MODULE3_PATH =
      System.getProperty("user.dir") + "/../libs/module3WithManifest-1.0.jar";
  private static final String MODULE4_PATH =
      System.getProperty("user.dir") + "/../libs/module4WithManifest-1.0.jar";

  private static final String GEODE_COMMONS_SERVICES_PATH =
      System.getProperty("user.dir") + "/../libs/geode-common-services-" + gemFireVersion + ".jar";
  private static final String GEODE_COMMONS_PATH =
      System.getProperty("user.dir") + "/../libs/geode-common-" + gemFireVersion + ".jar";

  private static ModuleDescriptor geodeCommonsServiceDescriptor;

  private static ModuleDescriptor geodeCommonDescriptor;

  @BeforeClass
  public static void setup() {
    geodeCommonsServiceDescriptor =
        new ModuleDescriptor.Builder("geode-common-services", gemFireVersion)
            .fromResourcePaths(GEODE_COMMONS_SERVICES_PATH)
            .build();

    geodeCommonDescriptor =
        new ModuleDescriptor.Builder("geode-common", gemFireVersion)
            .fromResourcePaths(GEODE_COMMONS_PATH)
            .build();
  }

  @Test
  public void findModuleSimpleJar() throws IOException, ModuleLoadException {
    ModuleDescriptor moduleDescriptor =
        new ModuleDescriptor.Builder("module1WithManifest", "1.0").fromResourcePaths(MODULE1_PATH)
            .build();

    ModuleFinder moduleFinder = new GeodeJarModuleFinder(LogManager.getLogger(), moduleDescriptor);
    ConcreteModuleSpec moduleSpec = (ConcreteModuleSpec) moduleFinder
        .findModule(moduleDescriptor.getName(), Module.getSystemModuleLoader());

    assertThat(moduleSpec.getName()).isEqualTo(moduleDescriptor.getName());
    assertThat(moduleSpec.getDependencies().length).isEqualTo(4);
    String[] expectedDependencies = new String[] {"log4j-core", "log4j-api", "jboss-modules",
        "module1WithManifest", "guava", "failureaccess", "listenablefuture",
        "jsr305", "checker-qual", "error_prone_annotations", "j2objc-annotations"};
    assertModuleResourcesEqual(moduleSpec, expectedDependencies);
  }

  @Test
  public void findModuleMultipleSourceJars() throws IOException, ModuleLoadException {
    ModuleDescriptor moduleDescriptor =
        new ModuleDescriptor.Builder("module1WithManifest", "1.0")
            .fromResourcePaths(MODULE1_PATH, MODULE2_PATH)
            .build();

    ModuleFinder moduleFinder = new GeodeJarModuleFinder(LogManager.getLogger(), moduleDescriptor);
    ConcreteModuleSpec moduleSpec = (ConcreteModuleSpec) moduleFinder
        .findModule(moduleDescriptor.getName(), Module.getSystemModuleLoader());

    assertThat(moduleSpec.getName()).isEqualTo(moduleDescriptor.getName());
    // This contain duplicate entries for 'geode-common-services'. This is because the underlying
    // moduleBuilder does check for duplicates
    assertThat(moduleSpec.getDependencies().length).isEqualTo(5);
    String[] expectedDependencies = new String[] {"log4j-core", "log4j-api", "jboss-modules",
        "module1WithManifest", "module2WithManifest", "guava", "failureaccess", "listenablefuture",
        "jsr305", "checker-qual", "error_prone_annotations", "j2objc-annotations"};

    assertModuleResourcesEqual(moduleSpec, expectedDependencies);
  }

  private void assertModuleResourcesEqual(ConcreteModuleSpec moduleSpec,
      String[] expectedDependencies) {
    Set<String> loadedResources = Arrays.stream(moduleSpec.getResourceLoaders())
        .map(resourceLoaderSpec -> resourceLoaderSpec.getResourceLoader().getLocation().toString())
        .collect(Collectors.toSet());
    assertThat(loadedResources.size()).isEqualTo(expectedDependencies.length);
    for (String expectedDependency : expectedDependencies) {
      boolean found = false;
      for (String loadedResource : loadedResources) {
        boolean contains = loadedResource.contains(expectedDependency);
        if (contains) {
          found = true;
        }
      }
      assertThat(found).isTrue();
    }
  }

  @Test
  public void findModuleJarWithDependencies() throws IOException, ModuleLoadException {
    ModuleDescriptor moduleDescriptor =
        new ModuleDescriptor.Builder("module1WithManifest", "1.0").fromResourcePaths(MODULE1_PATH)
            .dependsOnModules("exampleModule")
            .build();

    ModuleFinder moduleFinder = new GeodeJarModuleFinder(LogManager.getLogger(), moduleDescriptor);
    ConcreteModuleSpec moduleSpec = (ConcreteModuleSpec) moduleFinder
        .findModule(moduleDescriptor.getName(), Module.getSystemModuleLoader());

    assertThat(moduleSpec.getName()).isEqualTo(moduleDescriptor.getName());
    assertThat(moduleSpec.getDependencies().length).isEqualTo(5);
    String[] expectedDependencies = new String[] {"log4j-core", "log4j-api", "jboss-modules",
        "module1WithManifest", "guava", "failureaccess", "listenablefuture",
        "jsr305", "checker-qual", "error_prone_annotations", "j2objc-annotations"};
    assertModuleResourcesEqual(moduleSpec, expectedDependencies);
  }

  @Test
  public void loadJarFile() throws IOException, ModuleLoadException {
    ModuleDescriptor moduleDescriptor =
        new ModuleDescriptor.Builder("module1WithManifest", "1.0").fromResourcePaths(MODULE1_PATH)
            .build();

    ModuleLoader moduleLoader = new TestModuleLoader(Module.getSystemModuleLoader(),
        new ModuleFinder[] {
            new GeodeJarModuleFinder(LogManager.getLogger(),
                moduleDescriptor),
            new GeodeJarModuleFinder(LogManager.getLogger(),
                geodeCommonsServiceDescriptor),
            new GeodeJarModuleFinder(LogManager.getLogger(),
                geodeCommonDescriptor)
        });
    Module module = moduleLoader.loadModule(moduleDescriptor.getName());
    assertThat(module).isNotNull();
  }

  @Test
  public void loadMultipleJarFiles() throws IOException, ModuleLoadException {
    ModuleDescriptor moduleDescriptor =
        new ModuleDescriptor.Builder("module1WithManifest", "1.0")
            .fromResourcePaths(MODULE1_PATH, MODULE2_PATH)
            .build();

    ModuleLoader moduleLoader = new TestModuleLoader(Module.getSystemModuleLoader(),
        new ModuleFinder[] {
            new GeodeJarModuleFinder(LogManager.getLogger(), moduleDescriptor),
            new GeodeJarModuleFinder(LogManager.getLogger(), geodeCommonsServiceDescriptor),
            new GeodeJarModuleFinder(LogManager.getLogger(), geodeCommonDescriptor)
        });
    Module module = moduleLoader.loadModule(moduleDescriptor.getName());
    assertThat(module).isNotNull();
  }

  @Test
  public void loadJarFileWithDependencies() throws IOException, ModuleLoadException {

    ModuleDescriptor module1Descriptor =
        new ModuleDescriptor.Builder("module1WithManifest", "1.0").fromResourcePaths(MODULE1_PATH)
            .build();

    ModuleDescriptor module2Descriptor =
        new ModuleDescriptor.Builder("module2", "1.0")
            .fromResourcePaths(MODULE2_PATH)
            .build();

    ModuleLoader moduleLoader = new TestModuleLoader(Module.getSystemModuleLoader(),
        new ModuleFinder[] {
            new GeodeJarModuleFinder(LogManager.getLogger(), geodeCommonDescriptor),
            new GeodeJarModuleFinder(LogManager.getLogger(), geodeCommonsServiceDescriptor),
            new GeodeJarModuleFinder(LogManager.getLogger(),
                module1Descriptor),
            new GeodeJarModuleFinder(LogManager.getLogger(), module2Descriptor)
        });

    assertThat(moduleLoader.loadModule(geodeCommonDescriptor.getName())).isNotNull();
    assertThat(moduleLoader.loadModule(geodeCommonsServiceDescriptor.getName())).isNotNull();
    assertThat(moduleLoader.loadModule(module1Descriptor.getName())).isNotNull();
    Module module = moduleLoader.loadModule(module2Descriptor.getName());
    assertThat(module).isNotNull();
  }

  private static class TestModuleLoader extends DelegatingModuleLoader {

    public TestModuleLoader(ModuleLoader delegate, ModuleFinder[] finders) {
      super(delegate, finders);
    }
  }
}
