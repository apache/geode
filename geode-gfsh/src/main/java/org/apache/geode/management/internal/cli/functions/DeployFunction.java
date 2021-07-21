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

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import com.healthmarketscience.rmiio.RemoteInputStream;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.subject.Subject;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.management.internal.CacheElementOperation;
import org.apache.geode.management.internal.cli.domain.DeploymentInfo;
import org.apache.geode.management.internal.functions.CacheRealizationFunction;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.security.AuthenticationRequiredException;

public class DeployFunction implements InternalFunction<Object[]> {
  private static final Logger logger = LogService.getLogger();
  private static final long serialVersionUID = 1L;

  private static final String ID =
      "org.apache.geode.management.internal.cli.functions.DeployFunction";

  @Override
  public String getId() {
    return ID;
  }

  @Override
  @SuppressWarnings("deprecation")
  public void execute(FunctionContext<Object[]> context) {

    String memberIdentifier = "";

    try {
      final Object[] args = context.getArguments();
      @SuppressWarnings("unchecked")
      final List<String> jarFilenames = (List<String>) args[0];
      @SuppressWarnings("unchecked")
      final List<RemoteInputStream> jarStreams = (List<RemoteInputStream>) args[1];
      List<String> dependencies =
          args[2] == null ? Collections.emptyList() : Arrays.asList((String[]) args[2]);

      InternalCache cache = (InternalCache) context.getCache();
      DistributedMember member = cache.getDistributedSystem().getDistributedMember();

      String memberId = !member.getName().equals("") ? member.getName() : member.getId();
      memberIdentifier = memberId;

      List<DeploymentInfo> results = new LinkedList<>();

      List<Deployment> deployments;

      deployments = jarFilenames.stream().map(jarFileName -> new Deployment(jarFileName,
          getDeployedBy(cache), Instant.now().toString(), dependencies))
          .collect(Collectors.toList());

      for (int i = 0; i < deployments.size(); i++) {
        Deployment deployment = deployments.get(i);
        @SuppressWarnings("unchecked")
        Execution execution = FunctionService.onMember(member)
            .setArguments(
                Arrays.asList(deployment, CacheElementOperation.CREATE, jarStreams.get(i)));
        List<?> functionResult = (List<?>) execution
            .execute(new CacheRealizationFunction())
            .getResult();

        functionResult.forEach(entry -> {
          if (entry != null) {
            if (entry instanceof Throwable) {
              logger.warn("Error executing CacheRealizationFunction.", entry);
            } else if (entry instanceof RealizationResult) {
              RealizationResult realizationResult = (RealizationResult) entry;
              results.add(new DeploymentInfo(memberId,
                  deployment.getFileName(), realizationResult.getMessage()));
            }
          }
        });
      }

      CliFunctionResult result = new CliFunctionResult(memberIdentifier, results);
      context.getResultSender().lastResult(result);
    } catch (CacheClosedException cce) {
      CliFunctionResult result = new CliFunctionResult(memberIdentifier, false, null);
      context.getResultSender().lastResult(result);

    } catch (VirtualMachineError e) {
      org.apache.geode.SystemFailure.initiateFailure(e);
      throw e;

    } catch (Throwable throwable) {
      org.apache.geode.SystemFailure.checkFailure();
      logger.error("Could not deploy JAR file {}", throwable.getMessage(), throwable);

      CliFunctionResult result = new CliFunctionResult(memberIdentifier, throwable, null);
      context.getResultSender().lastResult(result);
    }
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

  private String getDeployedBy(InternalCache cache) {
    Subject subject = null;
    try {
      subject = cache.getSecurityService().getSubject();
    } catch (AuthenticationRequiredException e) {
      // ignored. No user logged in for the deployment
      // this would happen for offline commands like "start locator" and loading the cluster config
      // from a directory
      logger.debug("getDeployedBy: no user information is found.", e);
    }
    return subject == null ? null : subject.getPrincipal().toString();
  }
}
