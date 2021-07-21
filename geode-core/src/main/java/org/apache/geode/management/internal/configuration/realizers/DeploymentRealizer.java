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

import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.classloader.internal.ClassPathLoader;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.management.runtime.DeploymentInfo;
import org.apache.geode.services.result.ServiceResult;

public class DeploymentRealizer
    implements ConfigurationRealizer<Deployment, DeploymentInfo> {

  @Immutable
  private static final Logger logger = LogService.getLogger();

  static final String JAR_NOT_DEPLOYED = "Jar file not deployed on the server.";

  @Override
  public RealizationResult create(Deployment config, InternalCache cache) throws Exception {
    ServiceResult<Deployment> deploy = deploy(config);
    RealizationResult result = new RealizationResult();
    if (deploy.isFailure()) {
      result.setSuccess(false);
      result.setMessage(deploy.getErrorMessage());
    } else {
      Deployment deployment = deploy.getMessage();
      if (deployment == null) {
        result.setMessage("Already deployed");
      } else {
        result.setMessage(deployment.getFilePath());
      }
    }
    return result;
  }

  @Override
  public DeploymentInfo get(Deployment config, InternalCache cache) {
    DeploymentInfo info = new DeploymentInfo();
    ServiceResult<Deployment> deployed = getDeployed(config.getFileName());
    if (deployed.isSuccessful()) {
      info.setLastModified(deployed.getMessage().getDeployedTime());
      info.setJarLocation(deployed.getMessage().getFile().getAbsolutePath());
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
  ServiceResult<Deployment> getDeployed(String name) {
    return ClassPathLoader.getLatest().getJarDeploymentService().getDeployed(name);
  }

  @VisibleForTesting
  ServiceResult<Deployment> deploy(Deployment deployment) {
    return ClassPathLoader.getLatest().getJarDeploymentService().deploy(deployment);
  }
}
