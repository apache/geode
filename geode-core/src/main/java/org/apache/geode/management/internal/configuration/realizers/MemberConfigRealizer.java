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

import java.io.IOException;
import java.util.Optional;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalMembershipManager;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.configuration.MemberConfig;
import org.apache.geode.management.internal.cli.functions.GetMemberInformationFunction;
import org.apache.geode.management.runtime.MemberInformation;

public class MemberConfigRealizer
    implements ConfigurationRealizer<MemberConfig, MemberInformation> {
  private static final Logger logger = LogService.getLogger();

  @Override
  public RealizationResult create(MemberConfig config, InternalCache cache) {
    throw new IllegalStateException("Not supported");
  }

  @Override
  public MemberInformation get(MemberConfig config, InternalCache cache) {
    GetMemberInformationFunction getMemberInfoFunction = new GetMemberInformationFunction();
    InternalDistributedSystem system = (InternalDistributedSystem) cache.getDistributedSystem();
    DistributedMember member = system.getDistributedMember();

    try {
      MemberInformation memberInformation =
          getMemberInfoFunction.getMemberInformation(cache, system.getConfig(), member);
      if (member.getId().equals(getCoordinatorId((cache)))) {
        memberInformation.setCoordinator(true);
      }
      return memberInformation;
    } catch (IOException e) {
      logger.error("unable to get the member information. ", e);
      return null;
    }
  }

  @Override
  public RealizationResult update(MemberConfig config, InternalCache cache) {
    throw new IllegalStateException("Not supported");
  }

  @Override
  public RealizationResult delete(MemberConfig config, InternalCache cache) {
    throw new IllegalStateException("Not supported");
  }

  @VisibleForTesting
  String getCoordinatorId(InternalCache cache) {
    return Optional.ofNullable(cache.getDistributionManager())
        .map(DistributionManager::getMembershipManager)
        .map(InternalMembershipManager::getCoordinator)
        .map(DistributedMember::getId)
        .orElse(null);
  }
}
