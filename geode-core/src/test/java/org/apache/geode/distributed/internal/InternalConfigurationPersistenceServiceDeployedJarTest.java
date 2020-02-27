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

package org.apache.geode.distributed.internal;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.management.internal.configuration.domain.Configuration;

public class InternalConfigurationPersistenceServiceDeployedJarTest {
  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();
  private InternalConfigurationPersistenceService service;
  private Path workingDir;
  private Path stagingDir;
  private InternalCache cache;
  private DistributedLockService lockService;
  private Region configRegion;

  @Before
  public void before() throws Exception {
    configRegion = mock(Region.class);
    lockService = mock(DistributedLockService.class);
    cache = mock(InternalCache.class);
    when(cache.getRegion(any())).thenReturn(configRegion);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    when(member.getId()).thenReturn("member");
    when(cache.getMyId()).thenReturn(member);
    stagingDir = tempDir.newFolder("stagingDir").toPath();
    workingDir = tempDir.newFolder("workingDir").toPath();
    service =
        new InternalConfigurationPersistenceService(cache, lockService, null, workingDir, null);
  }

  @Test
  public void addPlainJarToThisLocator() throws Exception {
    Path jar1 = Files.createFile(stagingDir.resolve("abc.jar"));
    FileUtils.writeStringToFile(jar1.toFile(), "version1", "UTF-8");
    Path jar2 = Files.createFile(stagingDir.resolve("def.jar"));
    List<String> paths = Stream.of(jar1, jar2).map(Path::toString).collect(toList());
    service.addJarsToThisLocator("DEPLOYEDBY1", "DEPLOYEDTIME1", paths, null);
    assertThat(workingDir.resolve("cluster").toFile().list()).containsExactlyInAnyOrder("abc.jar",
        "def.jar");
    ArgumentCaptor<Configuration> argumentCaptor = ArgumentCaptor.forClass(Configuration.class);
    verify(configRegion).put(eq("cluster"), argumentCaptor.capture(), eq("member"));
    Configuration configuration = argumentCaptor.getValue();
    assertThat(configuration.getDeployments()).containsExactlyInAnyOrder(
        createDeployment("abc.jar", "DEPLOYEDBY1", "DEPLOYEDTIME1"),
        createDeployment("def.jar", "DEPLOYEDBY1", "DEPLOYEDTIME1"));

    // make sure in teh working dir, abc.jar has "version1" content
    assertThat(FileUtils.readFileToString(jar1.toFile(), "UTF-8")).isEqualTo("version1");

    // this makes sure the configuration of the first deploy is retained
    when(configRegion.get("cluster")).thenReturn(configuration);

    // deploy abc.jar again
    FileUtils.writeStringToFile(jar1.toFile(), "version2", "UTF-8");
    paths = Stream.of(jar1).map(Path::toString).collect(toList());
    service.addJarsToThisLocator("DEPLOYEDBY2", "DEPLOYEDTIME2", paths, null);
    assertThat(workingDir.resolve("cluster").toFile().list()).containsExactlyInAnyOrder("abc.jar",
        "def.jar");
    // make sure that in the working dir abc.jar has the new content
    assertThat(FileUtils.readFileToString(jar1.toFile(), "UTF-8")).isEqualTo("version2");

    verify(configRegion, times(2)).put(eq("cluster"), argumentCaptor.capture(), eq("member"));
    configuration = argumentCaptor.getValue();
    assertThat(configuration.getDeployments()).containsExactlyInAnyOrder(
        createDeployment("abc.jar", "DEPLOYEDBY2", "DEPLOYEDTIME2"),
        createDeployment("def.jar", "DEPLOYEDBY1", "DEPLOYEDTIME1"));
  }

  private Deployment createDeployment(String jarFileName, String deployedby, String deployedtime) {
    Deployment result = new Deployment();
    result.setFileName(jarFileName);
    result.setDeployedBy(deployedby);
    result.setDeployedTime(deployedtime);
    return result;
  }

  @Test
  public void addSemanticJarToThisLocator() throws Exception {
    Path jar1 = Files.createFile(stagingDir.resolve("abc-1.0.jar"));
    Path jar2 = Files.createFile(stagingDir.resolve("def-1.0.jar"));
    List<String> paths = Stream.of(jar1, jar2).map(Path::toString).collect(toList());
    service.addJarsToThisLocator("DEPLOYEDBY1", "DEPLOYEDTIME1", paths, null);
    assertThat(workingDir.resolve("cluster").toFile().list())
        .containsExactlyInAnyOrder("abc-1.0.jar", "def-1.0.jar");
    ArgumentCaptor<Configuration> argumentCaptor = ArgumentCaptor.forClass(Configuration.class);
    verify(configRegion).put(eq("cluster"), argumentCaptor.capture(), eq("member"));
    Configuration configuration = argumentCaptor.getValue();
    assertThat(configuration.getDeployments()).containsExactlyInAnyOrder(
        createDeployment("abc-1.0.jar", "DEPLOYEDBY1", "DEPLOYEDTIME1"),
        createDeployment("def-1.0.jar", "DEPLOYEDBY1", "DEPLOYEDTIME1"));

    // this makes sure the configuration of the first deploy is retained
    when(configRegion.get("cluster")).thenReturn(configuration);

    // deploy abc-1.1.jar
    Path jar3 = Files.createFile(stagingDir.resolve("abc-1.1.jar"));
    paths = Stream.of(jar3).map(Path::toString).collect(toList());
    service.addJarsToThisLocator("DEPLOYEDBY2", "DEPLOYEDTIME2", paths, null);
    assertThat(workingDir.resolve("cluster").toFile().list())
        .containsExactlyInAnyOrder("abc-1.1.jar", "def-1.0.jar");
    argumentCaptor = ArgumentCaptor.forClass(Configuration.class);
    verify(configRegion, times(2)).put(eq("cluster"), argumentCaptor.capture(), eq("member"));
    configuration = argumentCaptor.getValue();
    assertThat(configuration.getDeployments()).containsExactlyInAnyOrder(
        createDeployment("abc-1.1.jar", "DEPLOYEDBY2", "DEPLOYEDTIME2"),
        createDeployment("def-1.0.jar", "DEPLOYEDBY1", "DEPLOYEDTIME1"));

  }

  @Test
  public void addSemanticAndPlainJarToThisLocator() throws Exception {
    // deploy abc-1.0.jar and def-1.0.jar
    Path jar1 = Files.createFile(stagingDir.resolve("abc-1.0.jar"));
    Path jar2 = Files.createFile(stagingDir.resolve("def-1.0.jar"));
    List<String> paths = Stream.of(jar1, jar2).map(Path::toString).collect(toList());
    service.addJarsToThisLocator("DEPLOYEDBY1", "DEPLOYEDTIME1", paths, null);
    ArgumentCaptor<Configuration> argumentCaptor = ArgumentCaptor.forClass(Configuration.class);
    verify(configRegion).put(eq("cluster"), argumentCaptor.capture(), eq("member"));
    Configuration configuration = argumentCaptor.getValue();
    // this makes sure the configuration of the first deploy is retained
    when(configRegion.get("cluster")).thenReturn(configuration);
    Deployment defDeployment = createDeployment("def-1.0.jar", "DEPLOYEDBY1", "DEPLOYEDTIME1");
    assertThat(configuration.getDeployments()).containsExactlyInAnyOrder(
        createDeployment("abc-1.0.jar", "DEPLOYEDBY1", "DEPLOYEDTIME1"), defDeployment);

    // deploy abc.jar
    Path jar3 = Files.createFile(stagingDir.resolve("abc.jar"));
    paths = singletonList(jar3.toString());
    service.addJarsToThisLocator("DEPLOYEDBY2", "DEPLOYEDTIME2", paths, null);
    assertThat(workingDir.resolve("cluster").toFile().list())
        .containsExactlyInAnyOrder("abc.jar", "def-1.0.jar");
    verify(configRegion, times(2)).put(eq("cluster"), argumentCaptor.capture(), eq("member"));
    configuration = argumentCaptor.getValue();
    assertThat(configuration.getDeployments()).containsExactlyInAnyOrder(
        createDeployment("abc.jar", "DEPLOYEDBY2", "DEPLOYEDTIME2"), defDeployment);
  }

  @Test
  public void addsDeploymentsFromJarFileNames() {
    String deployedBy = "deployedBy";
    String deployedTime = Instant.now().toString();

    Collection<String> allFileNames = new HashSet<>();
    Collection<String> jarFileNames = asList("jar1.jar", "jar2.jar", "jar3.jar");
    allFileNames.addAll(jarFileNames);
    allFileNames.addAll(asList("nonJar.xml", "nonJar.pdf"));

    Configuration configuration = new Configuration();

    InternalConfigurationPersistenceService
        .loadDeploymentsFromFileNames(allFileNames, configuration, deployedBy, deployedTime);

    List<Deployment> expectedDeployments = jarFileNames.stream()
        .map(jarFileName -> new Deployment(jarFileName, deployedBy, deployedTime))
        .collect(toList());

    assertThat(configuration.getDeployments())
        .containsExactlyInAnyOrderElementsOf(expectedDeployments);
  }
}
