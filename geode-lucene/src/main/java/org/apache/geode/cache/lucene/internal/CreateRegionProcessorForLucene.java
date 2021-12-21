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

import java.util.Set;

import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.internal.cache.CacheDistributionAdvisee;
import org.apache.geode.internal.cache.CacheDistributionAdvisor;
import org.apache.geode.internal.cache.CacheServiceProfile;
import org.apache.geode.internal.cache.CreateRegionProcessor;

public class CreateRegionProcessorForLucene extends CreateRegionProcessor {

  public CreateRegionProcessorForLucene(CacheDistributionAdvisee newRegion) {
    super(newRegion);
  }

  @Override
  protected CreateRegionMessage getCreateRegionMessage(Set recps, ReplyProcessor21 proc,
      boolean useMcast) {
    CreateRegionMessage msg = new CreateRegionMessage(newRegion.getFullPath(),
        (CacheDistributionAdvisor.CacheProfile) newRegion.getProfile(), proc.getProcessorId());
    msg.concurrencyChecksEnabled = newRegion.getAttributes().getConcurrencyChecksEnabled();
    msg.setMulticast(useMcast);
    msg.setRecipients(recps);
    return msg;

  }

  public static class CreateRegionMessage extends CreateRegionProcessor.CreateRegionMessage {
    public CreateRegionMessage(String regionPath,
        CacheDistributionAdvisor.CacheProfile cacheProfile, int processorId) {
      this.regionPath = regionPath;
      profile = cacheProfile;
      this.processorId = processorId;
    }

    public CreateRegionMessage() {}

    @Override
    public int getDSFID() {
      return CREATE_REGION_MESSAGE_LUCENE;
    }


    @Override
    protected String getMissingProfileMessage(CacheServiceProfile profile,
        boolean existsInThisMember) {
      /*
       * Reindex operation
       * Null string return is treated as no error scenario
       * Since this is a reindex operation it is acceptable for a region to have a missing cache
       * service profile. It will eventually receive a create index command
       */

      return null;
    }
  }

}
