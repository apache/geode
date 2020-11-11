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



import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.collection.SortedSet;
import io.vavr.collection.TreeSet;
import org.apache.logging.log4j.Logger;
import org.jboss.modules.JDKModuleFinder;
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

  private SortedSet<Tuple2<String, ModuleFinder>> finders =
      TreeSet.of(new Tuple2<>("JDK", JDKModuleFinder.getInstance()));

  public DelegatingModuleFinder(Logger logger) {
    this.logger = logger;
  }

  /**
   * Adds a {@link ModuleFinder} to be used for finding modules.
   *
   * @param moduleName Name of the module associated with the {@link ModuleFinder}.
   * @param finder a {@link ModuleFinder} used to find the {@link ModuleSpec} for a specific module.
   */
  public void addModuleFinder(String moduleName, ModuleFinder finder) {
    if (moduleFinderAlreadyAdded(moduleName)) {
      logger.debug("ModuleFinder for module named: " + moduleName + " already added");
    } else {
      finders = finders.add(new Tuple2<>(moduleName, finder));
      logger.debug("Added finder " + finder);
    }
  }

  private boolean moduleFinderAlreadyAdded(String moduleName) {
    return !finders.toStream().filter(tuple -> tuple._1.equals(moduleName)).toList().isEmpty();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ModuleSpec findModule(String name, ModuleLoader delegateLoader)
      throws ModuleLoadException {
    for (Tuple2<String, ModuleFinder> finder : finders) {
      ModuleSpec moduleSpec = finder._2().findModule(name, delegateLoader);
      if (moduleSpec != null) {
        logger.debug(String.format("Found module specification for module named: %s ", name));
        return moduleSpec;
      }
    }
    logger.debug(String.format("No module specification for module named: %s found", name));
    return null;
  }

  /**
   * Removes a previously added {@link ModuleFinder} given its name.
   *
   * @param moduleName the name of the module to remove the {@link ModuleFinder} of.
   */
  public void removeModuleFinderForName(String moduleName) {
    List<Tuple2<String, ModuleFinder>> findersToRemove =
        finders.toStream().filter(tuple -> tuple._1.equals(moduleName)).toList();
    finders = finders.removeAll(findersToRemove);
  }
}
