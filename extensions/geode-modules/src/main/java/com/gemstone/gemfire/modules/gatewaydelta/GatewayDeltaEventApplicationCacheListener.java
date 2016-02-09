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
package com.gemstone.gemfire.modules.gatewaydelta;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventCallbackArgument;

import java.util.Properties;

public class GatewayDeltaEventApplicationCacheListener extends CacheListenerAdapter<String, GatewayDeltaEvent> implements Declarable {

  private final Cache cache;

  public GatewayDeltaEventApplicationCacheListener() {
    this.cache = CacheFactory.getAnyInstance();
  }

  public void afterCreate(EntryEvent<String, GatewayDeltaEvent> event) {
    System.out.println("GatewayDeltaApplierCacheListener event: " + event);
    EntryEventImpl eventImpl = (EntryEventImpl) event;
    if (this.cache.getLogger().fineEnabled()) {
      StringBuilder builder = new StringBuilder();
      builder.append("GatewayDeltaApplierCacheListener: Received event for ")
          .append(event.getKey())
          .append("->")
          .append(event.getNewValue())
          .append(".");
      this.cache.getLogger().fine(builder.toString());
    }

    // If the event is from a remote site, apply it to the session
    Object callbackArgument = eventImpl.getRawCallbackArgument();
    System.out.println("GatewayDeltaApplierCacheListener callbackArgument: " + callbackArgument);
    if (callbackArgument instanceof GatewaySenderEventCallbackArgument) {
      GatewayDeltaEvent delta = event.getNewValue();
      delta.apply(this.cache);
      System.out.println("Applied " + delta);
      if (this.cache.getLogger().fineEnabled()) {
        StringBuilder builder = new StringBuilder();
        builder.append("GatewayDeltaApplierCacheListener: Applied ").append(delta);
        this.cache.getLogger().fine(builder.toString());
      }
    }
  }

  public void init(Properties p) {
  }
}
