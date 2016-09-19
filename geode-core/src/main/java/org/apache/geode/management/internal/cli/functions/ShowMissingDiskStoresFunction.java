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
package org.apache.geode.management.internal.cli.functions;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.partitioned.ColocatedRegionDetails;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.cache.persistence.PersistentMemberManager;
import org.apache.geode.internal.cache.persistence.PersistentMemberPattern;

public class ShowMissingDiskStoresFunction extends FunctionAdapter implements InternalEntity {

  protected Cache getCache() {
    return CacheFactory.getAnyInstance();
  }

  @Override
  public void execute(FunctionContext context) {
    final Set<PersistentMemberPattern> memberMissingIDs = new HashSet<PersistentMemberPattern>();
    Set<ColocatedRegionDetails> missingColocatedRegions = new HashSet<ColocatedRegionDetails>();

    if (context == null) {
      throw new RuntimeException();
    }
    try {
      final Cache cache = getCache();

      if (cache instanceof InternalCache) {
        final InternalCache gemfireCache = (InternalCache) cache;

        final DistributedMember member = gemfireCache.getMyId();

        GemFireCacheImpl gfci = GemFireCacheImpl.getInstance();
        if(gfci != null && !gfci.isClosed()) {
          // Missing DiskStores
          PersistentMemberManager mm = gfci.getPersistentMemberManager();
          Map<String, Set<PersistentMemberID>> waitingRegions = mm.getWaitingRegions();
          for (Map.Entry<String, Set<PersistentMemberID>> entry : waitingRegions.entrySet()) {
            for(PersistentMemberID id : entry.getValue()) {
              memberMissingIDs.add(new PersistentMemberPattern(id));
            }
          }
          // Missing colocated regions
          Set<PartitionedRegion> prs = gfci.getPartitionedRegions();
          for (PartitionedRegion pr: prs) {
            List<String> missingChildRegions = pr.getMissingColocatedChildren();
            for (String child:missingChildRegions) {
              missingColocatedRegions.add(new ColocatedRegionDetails(member.getHost(), member.getName(),pr.getFullPath(), child));
            }
          }
        }

      }

      if (memberMissingIDs.isEmpty() && missingColocatedRegions.isEmpty()) {
        context.getResultSender().lastResult(null);
      } else {
        if (!memberMissingIDs.isEmpty()) {
          if (missingColocatedRegions.isEmpty()) {
            context.getResultSender().lastResult(memberMissingIDs);
          } else {
            context.getResultSender().sendResult(memberMissingIDs);
          }
        }
        if (!missingColocatedRegions.isEmpty()) {
          context.getResultSender().lastResult(missingColocatedRegions);
        }
      }
    }
    catch (Exception e) {
      context.getResultSender().sendException(e);
    }
  }

  @Override
  public String getId() {
    return getClass().getName();
  }
}

