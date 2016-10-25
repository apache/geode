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
package org.apache.geode.internal.cache;

import org.apache.geode.cache.CacheEvent;
import org.apache.geode.internal.cache.FilterRoutingInfo.FilterInfo;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.versions.VersionTag;

/**
 * A CacheEvent, but the isGenerateCallbacks() is hidden from public consumption
 *
 */
public interface InternalCacheEvent extends CacheEvent
{
  /**
   * Answers true if this event should generate user callbacks.
   * @return true if this event will generate user callbacks
   */
  public boolean isGenerateCallbacks();
  
  /**
   * Answers true if this event is from a client
   * @deprecated as of 5.7 use {@link #hasClientOrigin} instead.
   */
  @Deprecated
  public boolean isBridgeEvent();
  /**
   * Answers true if this event is from a client
   * @since GemFire 5.7
   */
  public boolean hasClientOrigin();
  
  /**
   * returns the ID associated with this event
   */
  public EventID getEventId();
  
  /**
   * Returns the Operation type.
   * @return eventType
   */
  public EnumListenerEvent getEventType();

  /**
   * sets the event type
   * @param operation the operation performed by this event
   */
  public void setEventType(EnumListenerEvent operation);
  
  /**
   * returns the bridge context for the event, if any
   */
  public ClientProxyMembershipID getContext();
  
  /**
   * set the client routing information for this event
   * @param info TODO
   */
  public void setLocalFilterInfo(FilterInfo info);
  
  /**
   * get the local routing information for this event
   */
  public FilterInfo getLocalFilterInfo();
  
  /**
   * get the version tag for the event
   */
  public VersionTag getVersionTag();

}
