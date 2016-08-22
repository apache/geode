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

package com.gemstone.gemfire.cache;

/**
 * Interface <code>InterestRegisterationListener</code> provides the ability for
 * applications to be notified of interest registration and unregistration
 * events. Instances must be implemented by applications and registered in
 * <code>CacheServer</code> VMs using the
 * {@link com.gemstone.gemfire.cache.server.CacheServer#registerInterestRegistrationListener
 * registerInterestRegistrationListener} API. The methods on an
 * <code>InterestRegisterationListener</code> are invoked synchronously with the
 * interest event in any <code>CacheServer</code> VM hosting the requesting
 * client's subscriptions.
 *
 * <p>Shown below is an example implementation.
 *
 * <pre>
 *import com.gemstone.gemfire.cache.InterestRegistrationEvent;
 *import com.gemstone.gemfire.cache.InterestRegistrationListener;
 *
 *public class TestInterestRegistrationListener implements InterestRegistrationListener {
 *
 *  public void afterRegisterInterest(InterestRegistrationEvent event) {
 *    System.out.println("afterRegisterInterest: " + event.getRegionName() + " -> " + event.getKeysOfInterest());
 *  }

 *  public void afterUnregisterInterest(InterestRegistrationEvent event) {
 *    System.out.println("afterUnregisterInterest: " + event.getRegionName() + " -> " + event.getKeysOfInterest());
 *  }
 *
 *  public void close() {}
 *}
 * </pre>
 *
 * Shown below is an example registration.
 *
 * <pre>
 *private void registerInterestRegistrationListener() {
 *  Cache cache = ...;
 *  CacheServer cs = cache.getCacheServers().iterator().next();
 *  InterestRegistrationListener listener = new TestInterestRegistrationListener();
 *  cs.registerInterestRegistrationListener(listener);
 *}
 * </pre>
 *
 *
 * @since GemFire 6.0
 * 
 * @see com.gemstone.gemfire.cache.server.CacheServer#registerInterestRegistrationListener registerInterestRegistrationListener
 * @see com.gemstone.gemfire.cache.server.CacheServer#unregisterInterestRegistrationListener unregisterInterestRegistrationListener
 */
public interface InterestRegistrationListener extends CacheCallback {

  /**
   * Handles an after register interest event.
   *
   * @param event the InterestRegistrationEvent
   */
  public void afterRegisterInterest(InterestRegistrationEvent event);

  /**
   * Handles an after unregister interest event.
   *
   * @param event the InterestRegistrationEvent
   */
  public void afterUnregisterInterest(InterestRegistrationEvent event);
}
