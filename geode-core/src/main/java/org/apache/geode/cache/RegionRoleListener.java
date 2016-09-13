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
package org.apache.geode.cache;

/**
 * A listener that can be implemented to handle region reliability membership 
 * events.  These are membership events that are specific to loss or gain of
 * required roles as defined by the region's {@link MembershipAttributes}.
 * <p>
 * Instead of implementing this interface it is recommended that you extend
 * the {@link org.apache.geode.cache.util.RegionRoleListenerAdapter} 
 * class.
 * 
 * 
 * @see AttributesFactory#setCacheListener
 * @see RegionAttributes#getCacheListener
 * @see AttributesMutator#setCacheListener
 * @deprecated this feature is scheduled to be removed
 */
public interface RegionRoleListener<K,V> extends CacheListener<K,V> {

  /**
   * Invoked when a required role has returned to the distributed system
   * after being absent.
   *
   * @param event describes the member that fills the required role.
   */
  public void afterRoleGain(RoleEvent<K,V> event);
  
  /**
   * Invoked when a required role is no longer available in the distributed
   * system.
   *
   * @param event describes the member that last filled the required role.
   */
  public void afterRoleLoss(RoleEvent<K,V> event);
  
}

