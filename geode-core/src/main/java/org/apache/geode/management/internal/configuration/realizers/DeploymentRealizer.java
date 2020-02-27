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

package org.apache.geode.management.internal.configuration.realizers;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import org.apache.commons.lang3.NotImplementedException;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.DeployedJar;
import org.apache.geode.internal.JarDeployer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.management.runtime.DeploymentInfo;

public class DeploymentRealizer
    implements ConfigurationRealizer<Deployment, DeploymentInfo> {

  static final String JAR_NOT_DEPLOYED = "Jar file not deployed on the server.";

  @Override
  public RealizationResult create(Deployment config, InternalCache cache) throws Exception {
    RealizationResult result = new RealizationResult();
    DeployedJar deployedJar = deploy(config.getFile());
    if (deployedJar == null) {
      result.setMessage("Already deployed");
    } else {
      result.setMessage(deployedJar.getFileCanonicalPath());
    }
    return result;
  }

  @Override
  public DeploymentInfo get(Deployment config, InternalCache cache) {
    Map<String, DeployedJar> deployedJars = getDeployedJars();
    DeploymentInfo info = new DeploymentInfo();
    String artifactId = JarDeployer.getArtifactId(config.getFileName());
    DeployedJar deployedJar = deployedJars.get(artifactId);
    if (deployedJar != null) {
      File file = deployedJar.getFile();
      info.setLastModified(getDateString(file.lastModified()));
      info.setJarLocation(file.getAbsolutePath());
    } else {
      info.setJarLocation(JAR_NOT_DEPLOYED);
    }
    return info;
  }

  @Override
  public boolean exists(Deployment config, InternalCache cache) {
    return false;
  }

  @Override
  public RealizationResult update(Deployment config, InternalCache cache) {
    throw new NotImplementedException("Not implemented");
  }

  @Override
  public RealizationResult delete(Deployment config, InternalCache cache) {
    throw new NotImplementedException("Not implemented");
  }

  @VisibleForTesting
  String getDateString(long milliseconds) {
    return Instant.ofEpochMilli(milliseconds).toString();
  }

  @VisibleForTesting
  Map<String, DeployedJar> getDeployedJars() {
    JarDeployer jarDeployer = ClassPathLoader.getLatest().getJarDeployer();
    return jarDeployer.getDeployedJars();
  }

  @VisibleForTesting
  DeployedJar deploy(File jarFile) throws IOException, ClassNotFoundException {
    return ClassPathLoader.getLatest().getJarDeployer().deploy(jarFile);
  }
}
