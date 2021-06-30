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

import java.util.List;

import org.jboss.modules.ModuleFinder;
import org.jboss.modules.ModuleLoader;
import org.jboss.modules.ModuleSpec;
import org.jboss.modules.ModuleSpecUtils;

import org.apache.geode.deployment.internal.modules.extensions.Extension;
import org.apache.geode.deployment.internal.modules.extensions.GeodeExtension;
import org.apache.geode.deployment.internal.modules.utils.ModuleSpecBuilderUtils;

public class GeodeExtensionModuleFinder implements ModuleFinder {

  private final Extension extension;

  public GeodeExtensionModuleFinder(Extension extension) {
    this.extension = extension;
  }

  @Override
  public ModuleSpec findModule(String name, ModuleLoader delegateLoader) {
    if (name.equals(extension.getName())) {
      ModuleSpec.Builder builder = ModuleSpecUtils.createBuilder(extension.getName(), true);
      builder = ModuleSpecBuilderUtils.addLocalDependencySpec(builder);
      if (extension instanceof GeodeExtension) {
        List<String> moduleDependencies = ((GeodeExtension) extension).getModuleDependencies();
        builder = ModuleSpecBuilderUtils.addModuleDependencies(builder, true, moduleDependencies);
      }

      return ModuleSpecUtils.addSystemClasspathDependency(builder.create());
    } else {
      return null;
    }
  }
}
