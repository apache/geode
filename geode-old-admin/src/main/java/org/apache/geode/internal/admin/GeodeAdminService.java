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
package org.apache.geode.internal.admin;



import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.admin.internal.SystemMemberCacheEventProcessor;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.remote.AdminConsoleDisconnectMessage;
import org.apache.geode.internal.cache.CacheService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.RegionListener;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.beans.CacheServiceMBeanBase;
import org.apache.geode.management.membership.ClientMembership;
import org.apache.geode.management.membership.ClientMembershipEvent;
import org.apache.geode.management.membership.ClientMembershipListener;

public class GeodeAdminService implements CacheService {
  private static final Logger logger = LogService.getLogger();
  private Cache cache;

  @Override
  public void init(Cache cache) {
    this.cache = cache;
    registerCacheListener((InternalCache) cache);
    registerClientMembershipListener(cache);
    registerMembershipListener((InternalCache) cache);
    SystemMemberCacheEventProcessor.send(cache, Operation.CACHE_CREATE);
  }

  private void registerCacheListener(InternalCache cache) {
    cache.addRegionListener(new RegionListener() {
      @Override
      public void afterCreate(Region region) {
        SystemMemberCacheEventProcessor.send(cache, region, Operation.REGION_CREATE);
      }

      @Override
      public void beforeDestroyed(Region region) {
        if (!cache.forcedDisconnect()) {
          SystemMemberCacheEventProcessor.send(cache, region,
              Operation.REGION_DESTROY);
        }

      }
    });
  }

  private void registerClientMembershipListener(Cache cache) {
    ClientMembershipListener listener = new ClientMembershipListener() {

      @Override
      public void memberJoined(ClientMembershipEvent event) {
        if (event.isClient()) {
          createAndSendMessage(event, ClientMembershipMessage.JOINED);
        }
      }

      @Override
      public void memberLeft(ClientMembershipEvent event) {
        if (event.isClient()) {
          createAndSendMessage(event, ClientMembershipMessage.LEFT);
        }
      }

      @Override
      public void memberCrashed(ClientMembershipEvent event) {
        if (event.isClient()) {
          createAndSendMessage(event, ClientMembershipMessage.CRASHED);
        }
      }

      /**
       * Method to create & send the ClientMembershipMessage to admin members. The message
       * is sent only if there are any admin members in the distribution system.
       *
       * @param event describes a change in client membership
       * @param type type of event - one of ClientMembershipMessage.JOINED,
       *        ClientMembershipMessage.LEFT, ClientMembershipMessage.CRASHED
       */
      private void createAndSendMessage(ClientMembershipEvent event, int type) {
        InternalDistributedSystem ds = null;
        Cache cacheInstance = cache;
        if (cacheInstance != null && !(cacheInstance instanceof CacheCreation)) {
          ds = (InternalDistributedSystem) cacheInstance.getDistributedSystem();
        } else {
          ds = InternalDistributedSystem.getAnyInstance();
        }

        // ds could be null
        if (ds != null && ds.isConnected()) {
          DistributionManager dm = ds.getDistributionManager();
          Set adminMemberSet = dm.getAdminMemberSet();

          /* check if there are any admin members at all */
          if (!adminMemberSet.isEmpty()) {
            DistributedMember member = event.getMember();

            ClientMembershipMessage msg = new ClientMembershipMessage(event.getMemberId(),
                member == null ? null : member.getHost(), type);

            msg.setRecipients(adminMemberSet);
            dm.putOutgoing(msg);
          }
        }
      }
    };

    ClientMembership.registerClientMembershipListener(listener);
  }

  private void registerMembershipListener(InternalCache cache) {
    cache.getDistributionManager().addMembershipListener(new MembershipListener() {
      @Override
      public void memberJoined(DistributionManager distributionManager,
          InternalDistributedMember id) {

      }

      @Override
      public void memberDeparted(DistributionManager distributionManager,
          InternalDistributedMember id, boolean crashed) {
        boolean wasAdmin = distributionManager.getAdminMemberSet().contains(id);
        if (wasAdmin) {
          // Pretend we received an AdminConsoleDisconnectMessage from the console that
          // is no longer in the JavaGroup view.
          // He must have died without sending a ShutdownMessage.
          // This fixes bug 28454.
          AdminConsoleDisconnectMessage message = new AdminConsoleDisconnectMessage();
          message.setSender(id);
          message.setCrashed(crashed);
          message.setAlertListenerExpected(true);
          message.setIgnoreAlertListenerRemovalFailure(true); // we don't know if it was a listener
          // so
          // don't issue a warning
          message.setRecipient(distributionManager.getId());
          message.setReason(""); // added for #37950
          message.process((ClusterDistributionManager) distributionManager);
        }

      }

      @Override
      public void memberSuspect(DistributionManager distributionManager,
          InternalDistributedMember id,
          InternalDistributedMember whoSuspected, String reason) {

      }

      @Override
      public void quorumLost(DistributionManager distributionManager,
          Set<InternalDistributedMember> failures,
          List<InternalDistributedMember> remaining) {

      }
    });
  }


  @Override
  public Class<? extends CacheService> getInterface() {
    return GeodeAdminService.class;
  }

  @Override
  public CacheServiceMBeanBase getMBean() {
    return null;
  }

  @Override
  public void close() {
    try {
      SystemMemberCacheEventProcessor.send(cache, Operation.CACHE_CLOSE);
    } catch (CancelException ignore) {
      if (logger.isDebugEnabled()) {
        logger.debug("Ignored cancellation while notifying admins");
      }
    }
  }
}
