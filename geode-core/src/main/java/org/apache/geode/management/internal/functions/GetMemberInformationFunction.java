/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.functions;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.internal.cache.CacheClientStatus;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.cache.tier.InternalClientMembership;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.net.SSLConfig;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.management.internal.util.ManagementUtils;
import org.apache.geode.management.runtime.CacheServerInfo;
import org.apache.geode.management.runtime.MemberInformation;

/***
 *
 * since 7.0
 */

public class GetMemberInformationFunction implements InternalFunction {

  private static final long serialVersionUID = 1404642539058875565L;

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

  /* Read only function */
  @Override
  public boolean optimizeForWrite() {
    return false;
  }

  @Override
  public void execute(FunctionContext functionContext) {
    try {
      Cache cache = functionContext.getCache();

      InternalDistributedSystem system = (InternalDistributedSystem) cache.getDistributedSystem();
      DistributionConfig config = system.getConfig();
      DistributedMember member =
          ManagementUtils.getDistributedMemberByNameOrId(functionContext.getMemberName(),
              (InternalCache) functionContext.getCache());

      MemberInformation memberInfo = getMemberInformation(cache, config, member);
      functionContext.getResultSender().lastResult(memberInfo);
    } catch (Exception e) {
      functionContext.getResultSender().sendException(e);
    }
  }

  public MemberInformation getMemberInformation(Cache cache, DistributionConfig config,
      DistributedMember member) throws IOException {
    MemberInformation memberInfo = new MemberInformation();

    memberInfo.setMemberName(member.getName());
    memberInfo.setId(member.getId());
    memberInfo.setHost(member.getHost());
    memberInfo.setProcessId(member.getProcessId());

    SSLConfig sslConfig = SSLConfigurationFactory.getSSLConfigForComponent(config,
        SecurableCommunicationChannel.WEB);
    memberInfo.setWebSSL(sslConfig.isEnabled());
    memberInfo.setSecured(StringUtils.isNotBlank(config.getSecurityManager()));
    memberInfo.setGroups(config.getGroups());
    memberInfo.setLogFilePath(config.getLogFile().getCanonicalPath());
    memberInfo.setStatArchiveFilePath(config.getStatisticArchiveFile().getCanonicalPath());
    memberInfo.setWorkingDirPath(System.getProperty("user.dir"));
    memberInfo.setCacheXmlFilePath(config.getCacheXmlFile().getCanonicalPath());
    memberInfo.setLocators(config.getLocators());
    memberInfo.setServerBindAddress(config.getServerBindAddress());
    memberInfo.setOffHeapMemorySize(config.getOffHeapMemorySize());
    memberInfo.setHttpServicePort(config.getHttpServicePort());
    memberInfo.setHttpServiceBindAddress(config.getHttpServiceBindAddress());

    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage memUsage = memoryMXBean.getHeapMemoryUsage();
    memberInfo.setHeapUsage(bytesToMeg(memUsage.getUsed()));
    memberInfo.setMaxHeapSize(bytesToMeg(memUsage.getMax()));
    memberInfo.setInitHeapSize(bytesToMeg(memUsage.getInit()));
    memberInfo.setHostedRegions(ManagementUtils.getAllRegionNames(cache));

    List<CacheServer> csList = cache.getCacheServers();

    InternalDistributedMember internalMember = (InternalDistributedMember) member;
    if (internalMember.getVmKind() == MemberIdentifier.LOCATOR_DM_TYPE) {
      memberInfo.setServer(false);
      InternalLocator locator = InternalLocator.getLocator();
      if (locator != null) {
        memberInfo.setLocatorPort(locator.getPort());
      }

      LocatorLauncher.LocatorState state = LocatorLauncher.getLocatorState();
      if (state != null) {
        memberInfo.setStatus(state.getStatus().getDescription());
      }
    } else {
      memberInfo.setServer(true);
      Iterator<CacheServer> iters = csList.iterator();
      while (iters.hasNext()) {
        CacheServer cs = iters.next();
        CacheServerInfo cacheServerInfo = new CacheServerInfo();
        cacheServerInfo.setBindAddress(cs.getBindAddress());
        cacheServerInfo.setPort(cs.getPort());
        cacheServerInfo.setRunning(cs.isRunning());
        cacheServerInfo.setMaxConnections(cs.getMaxConnections());
        cacheServerInfo.setMaxThreads(cs.getMaxThreads());
        memberInfo.addCacheServerInfo(cacheServerInfo);
      }
      Map<ClientProxyMembershipID, CacheClientStatus> allConnectedClients =
          InternalClientMembership.getStatusForAllClientsIgnoreSubscriptionStatus();
      Iterator<ClientProxyMembershipID> it = allConnectedClients.keySet().iterator();
      int numConnections = 0;

      while (it.hasNext()) {
        CacheClientStatus status = allConnectedClients.get(it.next());
        numConnections = numConnections + status.getNumberOfConnections();
      }
      memberInfo.setClientCount(numConnections);

      ServerLauncher.ServerState state = ServerLauncher.getServerState();
      if (state != null) {
        memberInfo.setStatus(state.getStatus().getDescription());
      }
    }
    return memberInfo;
  }


  private long bytesToMeg(long bytes) {
    return bytes / (1024L * 1024L);
  }
}
