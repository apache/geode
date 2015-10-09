/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.functions;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.CacheClientStatus;
import com.gemstone.gemfire.internal.cache.tier.InternalClientMembership;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.domain.CacheServerInfo;
import com.gemstone.gemfire.management.internal.cli.domain.MemberInformation;

/***
 * 
 * @author bansods
 * since 7.0
 */

public class GetMemberInformationFunction extends FunctionAdapter implements InternalEntity {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  @Override
  public String getId() {
    return GetMemberInformationFunction.class.getName();
  }

  @Override

  public boolean hasResult() {
    return true;
  }

  @Override 

  public boolean isHA() {
    return true;
  }

  @Override
  /**
   * Read only function
   */
  public boolean optimizeForWrite() {
    return false;
  }

  @Override
  public void execute(FunctionContext functionContext) {
    try {
      Cache cache = CacheFactory.getAnyInstance();

      /*TODO: 
       * 1) Get the CPU usage%
       */

      InternalDistributedSystem system = (InternalDistributedSystem) cache.getDistributedSystem();
      DistributionConfig 	config = system.getConfig();
      String 	serverBindAddress     = config.getServerBindAddress();
      MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

      MemberInformation memberInfo = new MemberInformation();
      memberInfo.setGroups(config.getGroups());
      memberInfo.setLogFilePath(config.getLogFile().getCanonicalPath());
      memberInfo.setStatArchiveFilePath(config.getStatisticArchiveFile().getCanonicalPath());
      memberInfo.setWorkingDirPath(System.getProperty("user.dir"));
      memberInfo.setCacheXmlFilePath(config.getCacheXmlFile().getCanonicalPath());
      memberInfo.setLocators(config.getLocators());
      memberInfo.setServerBindAddress(serverBindAddress);
      memberInfo.setOffHeapMemorySize(config.getOffHeapMemorySize());

      MemoryUsage memUsage = memoryMXBean.getHeapMemoryUsage();
      memberInfo.setHeapUsage(Long.toString(bytesToMeg(memUsage.getUsed())));
      memberInfo.setMaxHeapSize(Long.toString(bytesToMeg(memUsage.getMax())));
      memberInfo.setInitHeapSize(Long.toString(bytesToMeg(memUsage.getInit())));
      memberInfo.setHostedRegions(CliUtil.getAllRegionNames());
      
      List<CacheServer> csList = cache.getCacheServers();
      
      //A member is a server only if it has a cacheserver
      if (csList != null) {
        memberInfo.setServer(true);
        Iterator<CacheServer> iters = csList.iterator();
        while (iters.hasNext()) {
          CacheServer cs = iters.next();

          String bindAddress = cs.getBindAddress();
          int port = cs.getPort();
          boolean isRunning = cs.isRunning();

          CacheServerInfo cacheServerInfo = new CacheServerInfo(bindAddress, port, isRunning);
          memberInfo.addCacheServerInfo(cacheServerInfo);
        }
        Map<ClientProxyMembershipID, CacheClientStatus> allConnectedClients = InternalClientMembership
            .getStatusForAllClientsIgnoreSubscriptionStatus();
        Iterator<ClientProxyMembershipID> it = allConnectedClients.keySet().iterator();
        int numConnections = 0;

        while (it.hasNext()) {
          CacheClientStatus status = allConnectedClients.get(it.next());
          numConnections = numConnections + status.getNumberOfConnections();
        }
        memberInfo.setClientCount(numConnections);
      } else {
        memberInfo.setServer(false);
      }
      functionContext.getResultSender().lastResult(memberInfo);
    } catch (CacheClosedException e) {
      functionContext.getResultSender().sendException(e);
    } catch (Exception e) {
      functionContext.getResultSender().sendException(e);
    }
  }
  
  private long bytesToMeg(long bytes) {
    return bytes/(1024L * 1024L);
  }
}
