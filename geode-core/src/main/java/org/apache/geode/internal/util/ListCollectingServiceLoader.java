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
package org.apache.geode.internal.util;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.services.module.ModuleService;
import org.apache.geode.services.result.ModuleServiceResult;

/**
 * Implements {@link CollectingServiceLoader} by returning a {@link List} of all currently loadable
 * implementations of the given service interface.
 */
public class ListCollectingServiceLoader<S> implements CollectingServiceLoader<S> {
  private static final Logger logger = LogManager.getLogger();

  private final ModuleService moduleService;

  @VisibleForTesting
  public ListCollectingServiceLoader(ModuleService moduleService) {
    this.moduleService = moduleService;
  }

  @Override
  public Collection<S> loadServices(Class<S> service) {
    ModuleServiceResult<Set<S>> moduleServiceResult = moduleService.loadService(service);

    if (moduleServiceResult.isSuccessful()) {
      return moduleServiceResult.getMessage();
    } else {
      logger.warn(moduleServiceResult.getErrorMessage());
    }

    return Collections.EMPTY_LIST;
  }
}
