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

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.management.internal.configuration.domain.Configuration;

public class DeploymentManagerTest {
  @Test
  public void listWithNullJarNameReturnsAllDeployedJarsForGroup() {

    Set<Deployment> configuredDeployments = new HashSet<>(asList(
        new Deployment("jar1.jar", "deployedBy1", "deployedTime1"),
        new Deployment("jar2.jar", "deployedBy2", "deployedTime2"),
        new Deployment("jar3.jar", "deployedBy3", "deployedTime3")));

    Configuration configuration = mock(Configuration.class);
    when(configuration.getDeployments()).thenReturn(configuredDeployments);

    InternalConfigurationPersistenceService persistenceService =
        mock(InternalConfigurationPersistenceService.class);
    when(persistenceService.getConfiguration(any())).thenReturn(configuration);

    DeploymentManager manager = new DeploymentManager(persistenceService);

    Deployment filter = new Deployment();
    filter.setFileName(null);

    List<Deployment> result = manager.list(filter, "some-group");

    assertThat(result)
        .containsExactlyInAnyOrderElementsOf(configuredDeployments);
  }

  @Test
  public void listWithJarNameReturnsSingletonListConfiguredDeploymentForThatJar() {
    String requestedJarFile = "jar2.jar";
    Deployment expectedDeployment =
        new Deployment(requestedJarFile, "deployedBy2", "deployedTime2");

    Set<Deployment> configuredJarNames = new HashSet<>(asList(
        new Deployment("jar1.jar", "deployedBy1", "deployedTime1"),
        expectedDeployment,
        new Deployment("jar3.jar", "deployedBy3", "deployedTime3")));

    Configuration configuration = mock(Configuration.class);
    when(configuration.getDeployments()).thenReturn(configuredJarNames);

    InternalConfigurationPersistenceService persistenceService =
        mock(InternalConfigurationPersistenceService.class);
    when(persistenceService.getConfiguration(any())).thenReturn(configuration);

    DeploymentManager manager = new DeploymentManager(persistenceService);

    Deployment filter = new Deployment();
    filter.setFileName(requestedJarFile);

    List<Deployment> result = manager.list(filter, "some-group");

    assertThat(result).containsExactly(expectedDeployment);
  }

  @Test
  public void listWithJarNameReturnsEmptyListIfRequestedJarNotDeployed() {
    Configuration configuration = mock(Configuration.class);
    when(configuration.getDeployments()).thenReturn(emptySet());

    InternalConfigurationPersistenceService persistenceService =
        mock(InternalConfigurationPersistenceService.class);
    when(persistenceService.getConfiguration(any())).thenReturn(configuration);

    DeploymentManager manager = new DeploymentManager(persistenceService);

    Deployment filter = new Deployment();
    filter.setFileName("jarFileThatHasNotBeenDeployed.jar");

    List<Deployment> result = manager.list(filter, "some-group");

    assertThat(result).isEmpty();
  }

  @Test
  public void listNonExistentGroup() {
    InternalConfigurationPersistenceService persistenceService =
        mock(InternalConfigurationPersistenceService.class);
    when(persistenceService.getConfiguration("some-group")).thenReturn(null);

    DeploymentManager manager = new DeploymentManager(persistenceService);

    Deployment filter = new Deployment();

    List<Deployment> result = manager.list(filter, "some-group");

    assertThat(result).isEmpty();
  }
}
