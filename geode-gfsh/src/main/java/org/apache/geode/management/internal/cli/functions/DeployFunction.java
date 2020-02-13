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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.healthmarketscience.rmiio.RemoteInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.SystemFailure;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.DeployedJar;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.functions.CacheRealizationFunction;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class DeployFunction implements InternalFunction {
  private static final Logger logger = LogService.getLogger();

  public static final String ID = DeployFunction.class.getName();

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    // Declared here so that it's available when returning a Throwable
    String memberId = "";
    File stagingDir = null;

    try {
      final Object[] args = (Object[]) context.getArguments();
      final List<String> jarFilenames = (List<String>) args[0];
      final List<RemoteInputStream> jarStreams = (List<RemoteInputStream>) args[1];

      InternalCache cache = (InternalCache) context.getCache();
      DistributedMember member = cache.getDistributedSystem().getDistributedMember();

      memberId = member.getId();
      // If they set a name use it instead
      if (!member.getName().equals("")) {
        memberId = member.getName();
      }

      Set<File> stagedFiles = CacheRealizationFunction.stageFileContent(jarFilenames, jarStreams);
      stagingDir = stagedFiles.stream().findFirst().get().getParentFile();

      List<String> deployedList = new ArrayList<>();
      List<DeployedJar> jarClassLoaders =
          ClassPathLoader.getLatest().getJarDeployer().deploy(stagedFiles);
      for (int i = 0; i < jarFilenames.size(); i++) {
        deployedList.add(jarFilenames.get(i));
        // if deploy(jar) returns null, i.e. the staged file bytes matched the latest
        // deployed version
        if (jarClassLoaders.get(i) != null) {
          deployedList.add(jarClassLoaders.get(i).getFileCanonicalPath());
        } else {
          deployedList.add("Already deployed");
        }
      }

      CliFunctionResult result =
          new CliFunctionResult(memberId, deployedList.toArray(new String[0]));
      context.getResultSender().lastResult(result);

    } catch (IOException ex) {
      CliFunctionResult result =
          new CliFunctionResult(memberId, ex, "error staging jars for deployment");
      context.getResultSender().lastResult(result);
    } catch (CacheClosedException cce) {
      CliFunctionResult result = new CliFunctionResult(memberId, false, null);
      context.getResultSender().lastResult(result);

    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;

    } catch (Throwable th) {
      SystemFailure.checkFailure();
      logger.error("Could not deploy JAR file {}", th.getMessage(), th);

      CliFunctionResult result = new CliFunctionResult(memberId, th, null);
      context.getResultSender().lastResult(result);
    } finally {
      deleteStagingDir(stagingDir);
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

  private void deleteStagingDir(File stagingDir) {
    if (stagingDir == null) {
      return;
    }

    try {
      FileUtils.deleteDirectory(stagingDir);
    } catch (IOException iox) {
      logger.error("Unable to delete staging directory: {}", iox.getMessage());
    }
  }
}
