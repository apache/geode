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

package org.apache.geode.management.internal.configuration.mutators;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.configuration.AbstractConfiguration;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.management.internal.configuration.domain.Configuration;

public class DeploymentManager implements ConfigurationManager<Deployment> {
  private final InternalConfigurationPersistenceService persistenceService;

  public DeploymentManager(InternalConfigurationPersistenceService persistenceService) {
    this.persistenceService = persistenceService;
  }

  @Override
  public void add(Deployment config, String groupName) throws Exception {
    persistenceService.addJarsToThisLocator(
        Collections.singletonList(config.getFile().getAbsolutePath()),
        new String[] {AbstractConfiguration.getGroupName(config.getGroup())});
  }

  @Override
  public void delete(Deployment config, String groupName) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void update(Deployment config, String groupName) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public List<Deployment> list(Deployment filter, String groupName) {
    Configuration configuration = persistenceService.getConfiguration(groupName);
    if (configuration == null) {
      return emptyList();
    }

    return configuration.getDeployments().stream()
        .filter(deploymentsForJarName(filter.getFileName()))
        .collect(toList());
  }

  private static Predicate<Deployment> deploymentsForJarName(String jarFileName) {
    return jarFileName == null ? d -> true : d -> d.getFileName().equals(jarFileName);
  }
}
