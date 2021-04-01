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
package org.apache.geode.management.internal.cli.functions;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.deployment.internal.JarDeploymentService;
import org.apache.geode.deployment.internal.JarDeploymentServiceFactory;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.management.internal.cli.domain.DeploymentInfo;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.services.result.ServiceResult;

public class UndeployFunction implements InternalFunction<Object[]> {
  private static final Logger logger = LogService.getLogger();
  private static final long serialVersionUID = 1L;
  private static final String ID =
      "org.apache.geode.management.internal.cli.functions.UndeployFunction";

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public void execute(FunctionContext<Object[]> context) {
    // Declared here so that it's available when returning a Throwable
    String memberId = "";

    try {
      final Object[] args = context.getArguments();
      final String[] jarFilenameList = (String[]) args[0]; // Comma separated
      final String[] deploymentNameList = (String[]) args[1];
      InternalCache cache = (InternalCache) context.getCache();

      DistributedMember member = cache.getDistributedSystem().getDistributedMember();

      memberId = member.getId();
      // If they set a name use it instead
      if (!member.getName().equals("")) {
        memberId = member.getName();
      }

      List<DeploymentInfo> undeployedJars = new LinkedList<>();
      if (ArrayUtils.isNotEmpty(jarFilenameList)) {
        undeployedJars.addAll(undeployByFileNames(memberId, jarFilenameList));
      } else if (ArrayUtils.isNotEmpty(deploymentNameList)) {
        undeployedJars.addAll(undeployByDeploymentName(memberId, deploymentNameList));
      } else {
        // With no jars or deployments specified, all the deployed jars need to be removed
        undeployedJars.addAll(undeployAll(memberId));
      }

      CliFunctionResult result = new CliFunctionResult(memberId, undeployedJars, null);
      context.getResultSender().lastResult(result);

    } catch (Exception cce) {
      logger.error(cce.getMessage(), cce);
      CliFunctionResult result = new CliFunctionResult(memberId, false, null);
      context.getResultSender().lastResult(result);
    }
  }

  private List<DeploymentInfo> undeployAll(String memberId) {
    final JarDeploymentService jarDeploymentService =
        JarDeploymentServiceFactory.getJarDeploymentServiceInstance();
    List<DeploymentInfo> undeployedJars = new LinkedList<>();
    jarDeploymentService.listDeployed().forEach(deployment -> undeployedJars
        .addAll(undeployByDeploymentName(memberId, deployment.getDeploymentName())));
    return undeployedJars;
  }

  private List<DeploymentInfo> undeployByDeploymentName(String memberId,
      String... deploymentNames) {
    final JarDeploymentService jarDeploymentService =
        JarDeploymentServiceFactory.getJarDeploymentServiceInstance();
    List<DeploymentInfo> undeployedJars = new LinkedList<>();
    for (String deploymentName : deploymentNames) {
      logger.debug("Undeploying jar for deploymentName: {}", deploymentName);
      ServiceResult<Deployment> serviceResult =
          jarDeploymentService.undeployByDeploymentName(deploymentName);
      if (serviceResult.isSuccessful()) {
        logger.debug("Undeployed jar: {}", serviceResult.getMessage());
        undeployedJars.add(new DeploymentInfo(memberId, serviceResult.getMessage()));
      } else {
        logger.debug("Failed to undeploy jar: {}", serviceResult.getMessage());
        undeployedJars
            .add(new DeploymentInfo(memberId, deploymentName, null,
                serviceResult.getErrorMessage()));
      }
    }
    return undeployedJars;
  }

  private List<DeploymentInfo> undeployByFileNames(String memberId, String... filesToUndeploy) {
    final JarDeploymentService jarDeploymentService =
        JarDeploymentServiceFactory.getJarDeploymentServiceInstance();
    List<DeploymentInfo> undeployedJars = new LinkedList<>();
    for (String fileName : filesToUndeploy) {
      logger.debug("Undeploying jar for fileName: {}", fileName);
      ServiceResult<Deployment> serviceResult = jarDeploymentService.undeployByFileName(fileName);
      DeploymentInfo deploymentInfo;
      if (serviceResult.isSuccessful()) {
        logger.debug("Undeployed jar: {}", serviceResult.getMessage());
        deploymentInfo = new DeploymentInfo(memberId, serviceResult.getMessage());
      } else {
        logger.debug("Failed to undeploy jar: {}", serviceResult.getErrorMessage());
        deploymentInfo =
            new DeploymentInfo(memberId, null, fileName, serviceResult.getErrorMessage());
      }
      logger.debug("DeploymentInfo after undeployByFileNames {}", deploymentInfo);
      undeployedJars.add(deploymentInfo);
    }
    return undeployedJars;
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public boolean optimizeForWrite() {
    return false;
  }

  @Override
  public boolean isHA() {
    return false;
  }
}
