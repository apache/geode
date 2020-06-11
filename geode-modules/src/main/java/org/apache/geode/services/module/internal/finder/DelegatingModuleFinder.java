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

package org.apache.geode.services.module.internal.finder;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.logging.log4j.Logger;
import org.jboss.modules.ModuleFinder;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleLoader;
import org.jboss.modules.ModuleSpec;

/**
 * This {@link ModuleFinder} delegates the finding of modules to an internal List of
 * {@link ModuleFinder}.
 * This {@link ModuleFinder} is used to overcome the restriction from the {@link ModuleLoader} that
 * restricts the registered
 * {@link ModuleFinder}s at construction time, which does not allow for a dynamic set of
 * {@link ModuleFinder}s to be created at
 * runtime.
 *
 * @see ModuleFinder
 * @see ModuleLoader
 * @see ModuleSpec
 */
public class DelegatingModuleFinder implements ModuleFinder {
  private final Logger logger;

  private final List<ModuleFinder> finders = new CopyOnWriteArrayList<>();

  public DelegatingModuleFinder(Logger logger) {
    this.logger = logger;
  }

  public void addModuleFinder(ModuleFinder finder) {
    finders.add(finder);
    logger.debug("Added finder " + finder);
  }

  @Override
  public ModuleSpec findModule(String name, ModuleLoader delegateLoader)
      throws ModuleLoadException {
    for (ModuleFinder finder : finders) {
      ModuleSpec moduleSpec = finder.findModule(name, delegateLoader);
      if (moduleSpec != null) {
        logger.debug(String.format("Found module specification for module named: %s ", name));
        return moduleSpec;
      }
    }
    logger.debug(String.format("No module specification for module named: %s found", name));
    return null;
  }
}
