/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.cli.functions;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.JarClassLoader;
import com.gemstone.gemfire.internal.JarDeployer;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.logging.LogService;

public class UndeployFunction implements Function, InternalEntity {
  private static final Logger logger = LogService.getLogger();
  
  public static final String ID = UndeployFunction.class.getName();

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    // Declared here so that it's available when returning a Throwable
    String memberId = "";

    try {
      final Object[] args = (Object[]) context.getArguments();
      final String jarFilenameList = (String) args[0]; // Comma separated
      Cache cache = CacheFactory.getAnyInstance();

      final JarDeployer jarDeployer = new JarDeployer(((GemFireCacheImpl) cache).getDistributedSystem().getConfig().getDeployWorkingDir());
      
      DistributedMember member = cache.getDistributedSystem().getDistributedMember();

      memberId = member.getId();
      // If they set a name use it instead
      if (!member.getName().equals("")) {
        memberId = member.getName();
      }

      String[] undeployedJars = new String[0];
      if (jarFilenameList == null || jarFilenameList.equals("")) {
        final List<JarClassLoader> jarClassLoaders = jarDeployer.findJarClassLoaders();
        undeployedJars = new String[jarClassLoaders.size() * 2];
        int index = 0;
        for (JarClassLoader jarClassLoader : jarClassLoaders) {
          undeployedJars[index++] = jarClassLoader.getJarName();
          try {
            undeployedJars[index++] = jarDeployer.undeploy(jarClassLoader.getJarName());
          } catch (IllegalArgumentException iaex) {
            // It's okay for it to have have been uneployed from this server
            undeployedJars[index++] = iaex.getMessage();
          }
        }
      } else {
        List<String> undeployedList = new ArrayList<String>();
        StringTokenizer jarTokenizer = new StringTokenizer(jarFilenameList, ",");
        while (jarTokenizer.hasMoreTokens()) {
          String jarFilename = jarTokenizer.nextToken().trim();
          try {
            undeployedList.add(jarFilename);
            undeployedList.add(jarDeployer.undeploy(jarFilename));
          } catch (IllegalArgumentException iaex) {
            // It's okay for it to not have been deployed to this server
            undeployedList.add(iaex.getMessage());
          }
        }
        undeployedJars = undeployedList.toArray(undeployedJars);
      }

      CliFunctionResult result = new CliFunctionResult(memberId, undeployedJars);
      context.getResultSender().lastResult(result);
      
    } catch (CacheClosedException cce) {
      CliFunctionResult result = new CliFunctionResult(memberId, false, null);
      context.getResultSender().lastResult(result);
      
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
      
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      logger.error("Could not undeploy JAR file: {}", th.getMessage(), th);
      CliFunctionResult result = new CliFunctionResult(memberId, th, null);
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
