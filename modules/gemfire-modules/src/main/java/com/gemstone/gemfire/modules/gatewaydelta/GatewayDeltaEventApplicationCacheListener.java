/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.gatewaydelta;

import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.GatewayEventCallbackArgument;

public class GatewayDeltaEventApplicationCacheListener extends CacheListenerAdapter<String,GatewayDeltaEvent> implements Declarable {

  private final Cache cache;
  
  public GatewayDeltaEventApplicationCacheListener() {
    this.cache = CacheFactory.getAnyInstance();
  }
  
  public void afterCreate(EntryEvent<String,GatewayDeltaEvent> event) {
    System.out.println("GatewayDeltaApplierCacheListener event: " + event);
    EntryEventImpl eventImpl = (EntryEventImpl) event;
    if (this.cache.getLogger().fineEnabled()) {
      StringBuilder builder = new StringBuilder();
      builder
        .append("GatewayDeltaApplierCacheListener: Received event for ")
        .append(event.getKey())
        .append("->")
        .append(event.getNewValue())
        .append(".");
      this.cache.getLogger().fine(builder.toString());
    }

    // If the event is from a remote site, apply it to the session
    Object callbackArgument = eventImpl.getRawCallbackArgument();
    System.out.println("GatewayDeltaApplierCacheListener callbackArgument: " + callbackArgument);
    if (callbackArgument instanceof GatewayEventCallbackArgument) {
      GatewayDeltaEvent delta = event.getNewValue();
      delta.apply(this.cache);
      System.out.println("Applied " + delta);
      if (this.cache.getLogger().fineEnabled()) {
        StringBuilder builder = new StringBuilder();
        builder
          .append("GatewayDeltaApplierCacheListener: Applied ")
          .append(delta);
        this.cache.getLogger().fine(builder.toString());
      }
    }
  }

  public void init(Properties p) {}
}