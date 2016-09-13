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
package org.apache.geode.management.membership;

import org.apache.geode.internal.cache.tier.InternalClientMembership;

/**
 * Provides utility methods for registering and unregistering
 * ClientMembershipListeners in this process.
 *
 * @since GemFire 8.0
 */
public final class ClientMembership {

  private ClientMembership() {
  }

  /**
   * Registers a {@link ClientMembershipListener} for notification of connection
   * changes for CacheServer and clients.
   * 
   * @param listener
   *          a ClientMembershipListener to be registered
   */
  public static void registerClientMembershipListener(ClientMembershipListener listener) {
    InternalClientMembership.registerClientMembershipListener(listener);
  }

  /**
   * Removes registration of a previously registered
   * {@link ClientMembershipListener}.
   * 
   * @param listener
   *          a ClientMembershipListener to be unregistered
   */
  public static void unregisterClientMembershipListener(ClientMembershipListener listener) {
    InternalClientMembership.unregisterClientMembershipListener(listener);
  }

  /**
   * Returns an array of all the currently registered
   * <code>ClientMembershipListener</code>s. Modifications to the returned array
   * will not affect the registration of these listeners.
   * 
   * @return the registered <code>ClientMembershipListener</code>s; an empty
   *         array if no listeners
   */
  public static ClientMembershipListener[] getClientMembershipListeners() {
    return InternalClientMembership.getClientMembershipListeners();
  }

}
