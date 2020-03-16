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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.DeployedJar;
import org.apache.geode.internal.JarDeployer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class UndeployFunction implements InternalFunction<Object[]> {
  private static final Logger logger = LogService.getLogger();

  public static final String ID = UndeployFunction.class.getName();

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext<Object[]> context) {
    // Declared here so that it's available when returning a Throwable
    String memberId = "";

    try {
      final Object[] args = context.getArguments();
      final String[] jarFilenameList = (String[]) args[0]; // Comma separated
      InternalCache cache = (InternalCache) context.getCache();

      final JarDeployer jarDeployer = ClassPathLoader.getLatest().getJarDeployer();

      DistributedMember member = cache.getDistributedSystem().getDistributedMember();

      memberId = member.getId();
      // If they set a name use it instead
      if (!member.getName().equals("")) {
        memberId = member.getName();
      }

      List<String> jarNamesToUndeploy;
      if (ArrayUtils.isNotEmpty(jarFilenameList)) {
        jarNamesToUndeploy = Arrays.stream(jarFilenameList).collect(Collectors.toList());
      } else {
        final List<DeployedJar> jarClassLoaders = jarDeployer.findDeployedJars();
        jarNamesToUndeploy =
            jarClassLoaders.stream().map(DeployedJar::getDeployedFileName)
                .collect(Collectors.toList());
      }

      Map<String, String> undeployedJars = new HashMap<>();
      for (String jarName : jarNamesToUndeploy) {
        String jarLocation;
        try {
          jarLocation =
              ClassPathLoader.getLatest().getJarDeployer().undeploy(jarName);
        } catch (IOException | IllegalArgumentException iaex) {
          // It's okay for it to have have been undeployed from this server
          jarLocation = iaex.getMessage();
        }
        undeployedJars.put(jarName, jarLocation);
      }

      CliFunctionResult result = new CliFunctionResult(memberId, undeployedJars, null);
      context.getResultSender().lastResult(result);

    } catch (Exception cce) {
      logger.error(cce.getMessage(), cce);
      CliFunctionResult result = new CliFunctionResult(memberId, false, null);
      context.getResultSender().lastResult(result);
    }

  }

  @Override
  public String getId() {
    return ID;
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
