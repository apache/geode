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
package org.apache.geode.deployment.internal.modules.service;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.jboss.modules.Module;
import org.jboss.modules.ModuleLoadException;

import org.apache.geode.deployment.internal.modules.extensions.ExtensionContainer;
import org.apache.geode.deployment.internal.modules.extensions.impl.Application;
import org.apache.geode.deployment.internal.modules.loader.GeodeModuleLoader;

/**
 * This class is essentially a wrapper around {@link GeodeModuleLoader} that is used to load and
 * unload modules at runtime.
 */
public class GeodeJBossDeploymentService implements DeploymentService {
  private static final String DEFAULT_APPLICATION_NAME = "__default_app__";
  private static final String CORE_MODULE_NAME = "geode-core";
  private static final String EXTENSIONS_PROPERTIES_PATH = getExtensionPropertyPath();

  private static String getExtensionPropertyPath() {
    String environmentPath = System.getenv().get("GEODE_DISTRIBUTED_TEST_EXTENSIONS_PATH");
    if (environmentPath == null) {
      return System.getProperty("geode.deployments.extensions.path", "geodeExtensions.properties");
    }
    return environmentPath;
  }

  private final GeodeModuleLoader geodeModuleLoader;
  private final ExtensionContainer extensionContainer;

  public GeodeJBossDeploymentService() {
    this((GeodeModuleLoader) Module.getBootModuleLoader());
  }

  protected GeodeJBossDeploymentService(GeodeModuleLoader geodeModuleLoader) {
    this(geodeModuleLoader, new ExtensionContainer(geodeModuleLoader));
  }

  protected GeodeJBossDeploymentService(GeodeModuleLoader geodeModuleLoader,
      ExtensionContainer extensionContainer) {
    this.geodeModuleLoader = geodeModuleLoader;
    this.extensionContainer = extensionContainer;
  }

  public void loadGeodeExtensionsFromPropertiesFile(ClassLoader classLoader) {
    InputStream resourceAsStream = getExternalPropertiesInputStream(classLoader);
    if (resourceAsStream != null) {
      Properties properties = new Properties();
      try {
        properties.load(resourceAsStream);
        String geodeExtensions = properties.getProperty("geode.deployments.extensions", "");
        String[] extensions = geodeExtensions.split(",");
        registerGeodeExtensions(extensions);
      } catch (IOException e) {
        e.printStackTrace(System.err);
      }
    }
  }

  private InputStream getExternalPropertiesInputStream(ClassLoader classLoader) {
    InputStream resourceAsStream = classLoader.getResourceAsStream(EXTENSIONS_PROPERTIES_PATH);
    // if not found, try the classpath
    if (resourceAsStream == null) {
      resourceAsStream =
          getClass().getClassLoader().getResourceAsStream(EXTENSIONS_PROPERTIES_PATH);
    }
    if (resourceAsStream == null) {
      try {
        resourceAsStream = new FileInputStream(EXTENSIONS_PROPERTIES_PATH);
      } catch (FileNotFoundException e) {
        resourceAsStream = null;
      }
    }
    return resourceAsStream;
  }

  private boolean registerGeodeExtensions(String[] extensions) {
    return extensionContainer.registerGeodeExtensions(extensions);
  }

  public boolean registerModule(String moduleName, String filePath,
      List<String> moduleDependencyNames) {
    return this.registerModule(moduleName, DEFAULT_APPLICATION_NAME, filePath,
        moduleDependencyNames);
  }

  public boolean registerModule(String moduleName, String applicationName, String filePath,
      List<String> moduleDependencyNames) {
    validate(moduleName, filePath);
    if (moduleDependencyNames == null) {
      moduleDependencyNames = Collections.emptyList();
    }

    Application application = createApplicationIfAbsent(applicationName);
    if (application == null) {
      return false;
    }

    try {
      geodeModuleLoader.registerModule(moduleName, filePath, moduleDependencyNames);
      geodeModuleLoader.registerModulesAsDependencyOfModule(applicationName,
          application.getPathFilter(), moduleName);
      geodeModuleLoader.registerModulesAsDependencyOfModule(CORE_MODULE_NAME, applicationName);
    } catch (ModuleLoadException e) {
      throw new RuntimeException(e);
    }
    return true;
  }

  private Application createApplicationIfAbsent(String applicationName) {
    if (!extensionContainer.contains(applicationName)) {
      return extensionContainer.registerApplication(applicationName);
    }
    return (Application) extensionContainer.getExtensionByName(applicationName);
  }

  public boolean unregisterModule(String moduleName) {
    if (moduleName == null) {
      throw new IllegalArgumentException("Module name cannot be null");
    }
    try {
      geodeModuleLoader.unregisterModule(moduleName);
    } catch (ModuleLoadException e) {
      throw new RuntimeException(e);
    }
    return true;
  }

  @Override
  public boolean registerApplication(String applicationName) {
    return extensionContainer.registerApplication(applicationName) != null;
  }

  @Override
  public boolean registerGeodeExtension(String extensionName) {
    return extensionContainer.registerGeodeExtension(extensionName);
  }

  @Override
  public boolean unregisterExtension(String extensionName) {
    try {
      geodeModuleLoader.unregisterModule(extensionName);
    } catch (ModuleLoadException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  private void validate(String moduleName, String filePath) {
    if (moduleName == null) {
      throw new IllegalArgumentException("Module name cannot be null");
    }

    if (filePath == null) {
      throw new IllegalArgumentException("File path cannot be null");
    }
  }
}
