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
package org.apache.geode.cache.lucene.internal;

import java.util.Map;
import java.util.Set;

import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.internal.cache.CacheDistributionAdvisee;
import org.apache.geode.internal.cache.CacheDistributionAdvisor;
import org.apache.geode.internal.cache.CacheServiceProfile;
import org.apache.geode.internal.cache.CreateRegionProcessor;
import org.apache.geode.internal.cache.LocalRegion;

public class CreateRegionProcessorForLucene extends CreateRegionProcessor {

  public CreateRegionProcessorForLucene(CacheDistributionAdvisee newRegion) {
    super(newRegion);
  }

  @Override
  protected CreateRegionMessage getCreateRegionMessage(Set recps, ReplyProcessor21 proc,
      boolean useMcast) {
    CreateRegionMessage msg = new CreateRegionMessage(this.newRegion.getFullPath(),
        (CacheDistributionAdvisor.CacheProfile) this.newRegion.getProfile(), proc.getProcessorId());
    msg.concurrencyChecksEnabled = this.newRegion.getAttributes().getConcurrencyChecksEnabled();
    msg.setMulticast(useMcast);
    msg.setRecipients(recps);
    return msg;

  }

  public static class CreateRegionMessage extends CreateRegionProcessor.CreateRegionMessage {
    public CreateRegionMessage(String regionPath,
        CacheDistributionAdvisor.CacheProfile cacheProfile, int processorId) {
      this.regionPath = regionPath;
      this.profile = cacheProfile;
      this.processorId = processorId;
    }

    public CreateRegionMessage() {}

    @Override
    public int getDSFID() {
      return CREATE_REGION_MESSAGE_LUCENE;
    }


    @Override
    protected String checkCompatibility(CacheDistributionAdvisee rgn,
        CacheDistributionAdvisor.CacheProfile profile) {
      String cspResult = null;
      Map<String, CacheServiceProfile> myProfiles = ((LocalRegion) rgn).getCacheServiceProfiles();
      for (CacheServiceProfile remoteProfile : profile.cacheServiceProfiles) {
        CacheServiceProfile localProfile = myProfiles.get(remoteProfile.getId());
        if (localProfile != null) {
          cspResult = remoteProfile.checkCompatibility(rgn.getFullPath(), localProfile);
        }
        if (cspResult != null) {
          break;
        }
      }
      return cspResult;
    }
  }

}
