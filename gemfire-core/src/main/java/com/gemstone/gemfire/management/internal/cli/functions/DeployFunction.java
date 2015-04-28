/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.functions;

import java.util.ArrayList;
import java.util.List;

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

public class DeployFunction implements Function, InternalEntity {
  private static final Logger logger = LogService.getLogger();
  
  public static final String ID = DeployFunction.class.getName();

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    // Declared here so that it's available when returning a Throwable
    String memberId = "";

    try {
      final Object[] args = (Object[]) context.getArguments();
      final String[] jarFilenames = (String[]) args[0];
      final byte[][] jarBytes = (byte[][]) args[1];
      Cache cache = CacheFactory.getAnyInstance();

      final JarDeployer jarDeployer = new JarDeployer(((GemFireCacheImpl) cache).getDistributedSystem().getConfig().getDeployWorkingDir());

      DistributedMember member = cache.getDistributedSystem().getDistributedMember();

      memberId = member.getId();
      // If they set a name use it instead
      if (!member.getName().equals("")) {
        memberId = member.getName();
      }

      List<String> deployedList = new ArrayList<String>();
      JarClassLoader[] jarClassLoaders = jarDeployer.deploy(jarFilenames, jarBytes);
      for (int i = 0; i < jarFilenames.length; i++) {
        deployedList.add(jarFilenames[i]);
        if (jarClassLoaders[i] != null) {
          deployedList.add(jarClassLoaders[i].getFileCanonicalPath());
        } else {
          deployedList.add("Already deployed");
        }
      }
      
      CliFunctionResult result = new CliFunctionResult(memberId, deployedList.toArray(new String[0]));
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
