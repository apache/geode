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

package org.apache.geode.internal.cache.ha;

import java.util.Map;

import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;

/**
 * @since GemFire 5.7
 */
public interface HAContainerWrapper extends Map {

  public void cleanUp();

  public Object getEntry(Object key);

  public Object getKey(Object key);

  public String getName();

  public ClientProxyMembershipID getProxyID(String haRegionName);

  public Object putProxy(String haRegionName, CacheClientProxy proxy);

  public Object removeProxy(String haRegionName);
  
  public CacheClientProxy getProxy(String haRegionName);
 
  public Object putIfAbsent(Object key, Object value);
}
