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

package org.apache.geode.management.internal.configuration.mutators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.NotImplementedException;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.MembershipManager;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.configuration.MemberConfig;
import org.apache.geode.management.internal.cli.domain.CacheServerInfo;
import org.apache.geode.management.internal.cli.domain.MemberInformation;
import org.apache.geode.management.internal.cli.functions.GetMemberInformationFunction;

public class MemberConfigManager implements ConfigurationManager<MemberConfig> {

  private InternalCache cache;

  public MemberConfigManager(InternalCache cache) {
    this.cache = cache;
  }

  @Override
  public void add(MemberConfig config, CacheConfig existing) {
    throw new NotImplementedException("Not implemented");
  }

  @Override
  public void update(MemberConfig config, CacheConfig existing) {
    throw new NotImplementedException("Not implemented");
  }

  @Override
  public void delete(MemberConfig config, CacheConfig existing) {
    throw new NotImplementedException("Not implemented");
  }

  @Override
  public List<MemberConfig> list(MemberConfig filter, CacheConfig existing) {

    Set<DistributedMember> members =
        cache.getDistributionManager().getDistributionManagerIds()
            .stream().filter(internalDistributedMember -> (filter.getId() == null
                || filter.getId().equals(internalDistributedMember.getName())))
            .map(DistributedMember.class::cast).collect(Collectors.toSet());

    if (members.size() == 0) {
      return Collections.emptyList();
    }

    ArrayList<MemberInformation> output = getMemberInformation(members);

    return generateMemberConfigs(output);
  }

  private ArrayList<MemberInformation> getMemberInformation(Set<DistributedMember> members) {
    Execution execution = FunctionService.onMembers(members);
    ResultCollector<?, ?> resultCollector = execution.execute(new GetMemberInformationFunction());
    return (ArrayList<MemberInformation>) resultCollector.getResult();
  }

  private List<MemberConfig> generateMemberConfigs(ArrayList<MemberInformation> memberInformation) {

    final String coordinatorId = getCoordinatorId();
    List<MemberConfig> memberConfigs = new ArrayList<>();
    for (MemberInformation mInfo : memberInformation) {
      MemberConfig member = new MemberConfig();
      member.setId(mInfo.getName());
      member.setHost(mInfo.getHost());
      member.setPid(mInfo.getProcessId());
      member.setStatus(mInfo.getStatus());
      member.setInitialHeap(mInfo.getInitHeapSize());
      member.setMaxHeap(mInfo.getMaxHeapSize());
      member.setGroups(Arrays.asList(mInfo.getGroups().split(",")));
      member.setCoordinator(mInfo.getId().equals(coordinatorId));

      if (mInfo.isServer() && mInfo.getCacheServeInfo() != null) {
        for (CacheServerInfo info : mInfo.getCacheServeInfo()) {
          MemberConfig.CacheServerConfig csConfig = new MemberConfig.CacheServerConfig();
          csConfig.setPort(info.getPort());
          csConfig.setMaxConnections(info.getMaxConnections());
          csConfig.setMaxThreads(info.getMaxThreads());
          member.addCacheServer(csConfig);
        }
        member.setLocator(false);
      } else {
        member.setPort(mInfo.getLocatorPort());
        member.setLocator(true);
      }

      memberConfigs.add(member);
    }

    return memberConfigs;
  }

  private String getCoordinatorId() {
    final MembershipManager mmgr =
        cache.getDistributionManager().getMembershipManager();
    if (mmgr == null) {
      return null;
    }

    final DistributedMember coordinator = mmgr.getCoordinator();
    if (coordinator == null) {
      return null;
    }

    return coordinator.getId();
  }
}
