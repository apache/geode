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
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.wan.GatewaySenderEventCallbackArgument;

public class GatewayDeltaEventApplicationCacheListener
    extends CacheListenerAdapter<String, GatewayDeltaEvent> implements Declarable {

  private final Cache cache;

  GatewayDeltaEventApplicationCacheListener() {
    cache = CacheFactory.getAnyInstance();
  }

  @Override
  public void afterCreate(EntryEvent<String, GatewayDeltaEvent> event) {
    System.out.println("GatewayDeltaApplierCacheListener event: " + event);
    EntryEventImpl eventImpl = (EntryEventImpl) event;
    if (cache.getLogger().fineEnabled()) {
      String builder = "GatewayDeltaApplierCacheListener: Received event for " + event.getKey()
          + "->" + event.getNewValue() + ".";
      cache.getLogger().fine(builder);
    }

    // If the event is from a remote site, apply it to the session
    Object callbackArgument = eventImpl.getRawCallbackArgument();
    System.out.println("GatewayDeltaApplierCacheListener callbackArgument: " + callbackArgument);
    if (callbackArgument instanceof GatewaySenderEventCallbackArgument) {
      GatewayDeltaEvent delta = event.getNewValue();
      delta.apply(cache);
      System.out.println("Applied " + delta);

      if (cache.getLogger().fineEnabled()) {
        cache.getLogger().fine("GatewayDeltaApplierCacheListener: Applied " + delta);
      }
    }
  }
}
