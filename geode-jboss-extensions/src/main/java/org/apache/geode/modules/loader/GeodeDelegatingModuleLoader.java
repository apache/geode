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
package org.apache.geode.modules.loader;

import org.jboss.modules.DelegatingModuleLoader;
import org.jboss.modules.JDKModuleFinder;
import org.jboss.modules.LocalModuleFinder;
import org.jboss.modules.ModuleFinder;
import org.jboss.modules.ModuleLoader;

import org.apache.geode.modules.finder.GeodeCompositeModuleFinder;

public class GeodeDelegatingModuleLoader extends DelegatingModuleLoader implements AutoCloseable {

  private static final GeodeCompositeModuleFinder compositeModuleFinder =
      new GeodeCompositeModuleFinder("default", new LocalModuleFinder());

  public GeodeDelegatingModuleLoader() {
    super(new ModuleLoader(JDKModuleFinder.getInstance()), compositeModuleFinder);
  }

  public void registerModule(String moduleName, String path, String... modulesToDependOn) {

  }

  public void unregisterModule(String moduleName) {

  }

  public void reloadLinkingModule() {

  }

  @Override
  public void close() {
    final ModuleFinder[] finders = getFinders();
    assert finders.length == 1 && finders[0] instanceof LocalModuleFinder;
    ((LocalModuleFinder) finders[0]).close();
  }
}
