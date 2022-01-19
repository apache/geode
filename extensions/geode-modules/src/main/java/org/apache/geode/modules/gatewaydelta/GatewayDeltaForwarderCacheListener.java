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
package org.apache.geode.modules.gatewaydelta;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SerializedCacheValue;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.cache.EntryEventImpl;

public class GatewayDeltaForwarderCacheListener extends CacheListenerAdapter<String, GatewayDelta>
    implements Declarable {

  private final Cache cache;
  private Region<String, GatewayDeltaEvent> gatewayDeltaRegion;

  @SuppressWarnings("unused")
  public GatewayDeltaForwarderCacheListener() {
    this(CacheFactory.getAnyInstance());
  }

  public GatewayDeltaForwarderCacheListener(Cache cache) {
    this.cache = cache;
  }

  @Override
  public void afterCreate(EntryEvent<String, GatewayDelta> event) {
    // If the event is from the local site, create a 'create' event and send it to the
    // gateway delta region
    if (event.getCallbackArgument() == null) {
      if (cache.getLogger().fineEnabled()) {
        String builder = "GatewayDeltaForwarderCacheListener: Received create event for "
            + event.getKey() + "->" + event.getNewValue()
            + " that originated in the local site. Sending it to the remote site.";
        cache.getLogger().fine(
            builder);
      }

      // Distribute the create event to the gateway hub(s)
      String regionName = event.getRegion().getFullPath();
      String sessionId = event.getKey();
      SerializedCacheValue scv = event.getSerializedNewValue();
      if (scv == null) {
        getGatewayDeltaRegion().put(sessionId, new GatewayDeltaCreateEvent(regionName, sessionId,
            EntryEventImpl.serialize(event.getNewValue())));
      } else {
        getGatewayDeltaRegion().put(sessionId,
            new GatewayDeltaCreateEvent(regionName, sessionId, scv.getSerializedValue()));
      }
    } else {
      if (cache.getLogger().fineEnabled()) {
        String builder = "GatewayDeltaForwarderCacheListener: Received create event for "
            + event.getKey() + "->" + event.getNewValue()
            + " that originated in the remote site.";
        cache.getLogger().fine(builder);
      }
    }
  }

  @Override
  public void afterUpdate(EntryEvent<String, GatewayDelta> event) {
    // If the event is from the local site, create an 'update' event and send it to the
    // gateway delta region
    if (event.getCallbackArgument() == null) {
      if (cache.getLogger().fineEnabled()) {
        String builder = "GatewayDeltaForwarderCacheListener: Received update event for "
            + event.getKey() + "->" + event.getNewValue()
            + " that originated in the local site. Sending it to the remote site.";
        cache.getLogger().fine(
            builder);
      }

      // Distribute the update event to the gateway hub(s)
      GatewayDelta session = event.getNewValue();
      getGatewayDeltaRegion().put(event.getKey(), session.getCurrentGatewayDeltaEvent());

      // Reset the current delta
      session.setCurrentGatewayDeltaEvent(null);
    } else {
      if (cache.getLogger().fineEnabled()) {
        String builder = "GatewayDeltaForwarderCacheListener: Received update event for "
            + event.getKey() + "->" + event.getNewValue()
            + " that originated in the remote site.";
        cache.getLogger().fine(builder);
      }
    }
  }

  @Override
  public void afterDestroy(EntryEvent<String, GatewayDelta> event) {
    // If the event is from the local site, create a 'destroy' event and send it to the
    // gateway delta region
    if (event.getCallbackArgument() != null) {
      if (cache.getLogger().fineEnabled()) {
        String builder = "GatewayDeltaForwarderCacheListener: Received destroy event for "
            + event.getKey() + "->" + event.getNewValue()
            + " that originated in the local site. Sending it to the remote site.";
        cache.getLogger().fine(
            builder);
      }

      // Distribute the destroy event to the gateway hub(s)
      String sessionId = event.getKey();
      getGatewayDeltaRegion().put(sessionId,
          new GatewayDeltaDestroyEvent(event.getRegion().getFullPath(), sessionId));
    } else {
      if (cache.getLogger().fineEnabled()) {
        String builder = "GatewayDeltaForwarderCacheListener: Received destroy event for session "
            + event.getKey()
            + " that either expired or originated in the remote site.";
        cache.getLogger().fine(builder);
      }
    }
  }

  private Region<String, GatewayDeltaEvent> getGatewayDeltaRegion() {
    if (gatewayDeltaRegion == null) {
      gatewayDeltaRegion = createOrRetrieveGatewayDeltaRegion();
    }
    return gatewayDeltaRegion;
  }

  private Region<String, GatewayDeltaEvent> createOrRetrieveGatewayDeltaRegion() {
    Region<String, GatewayDeltaEvent> region =
        cache.getRegion(GatewayDelta.GATEWAY_DELTA_REGION_NAME);

    if (region == null) {
      region = cache.<String, GatewayDeltaEvent>createRegionFactory().setScope(Scope.LOCAL)
          .setDataPolicy(DataPolicy.EMPTY)
          .setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL))
          // TODO: Disabled for WAN
          // .setEnableGateway(true)
          .addCacheListener(new GatewayDeltaEventApplicationCacheListener())
          .create(GatewayDelta.GATEWAY_DELTA_REGION_NAME);
    }

    if (cache.getLogger().fineEnabled()) {
      String builder = "GatewayDeltaForwarderCacheListener: Created gateway delta region: "
          + region;
      cache.getLogger().fine(builder);
    }

    return region;
  }

  @Override
  public boolean equals(Object obj) {
    // This method is only implemented so that RegionCreator.validateRegion works properly.
    // The CacheListener comparison fails because two of these instances are not equal.
    if (this == obj) {
      return true;
    }

    return obj instanceof GatewayDeltaForwarderCacheListener;
  }

  @Override
  public int hashCode() {
    return GatewayDeltaForwarderCacheListener.class.hashCode();
  }
}
