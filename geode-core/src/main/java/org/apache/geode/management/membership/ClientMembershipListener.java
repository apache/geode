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

/**
 * A listener whose callback methods are invoked when this process detects
 * connection changes to CacheServers or clients.
 *
 * @see ClientMembership#registerClientMembershipListener
 *
 * @since GemFire 8.0
 */
public interface ClientMembershipListener {

  /**
   * Invoked when a client has connected to this process or when this process
   * has connected to a CacheServer.
   */
  public void memberJoined(ClientMembershipEvent event);

  /**
   * Invoked when a client has gracefully disconnected from this process or when
   * this process has gracefully disconnected from a CacheServer.
   */
  public void memberLeft(ClientMembershipEvent event);

  /**
   * Invoked when a client has unexpectedly disconnected from this process or
   * when this process has unexpectedly disconnected from a CacheServer.
   */
  public void memberCrashed(ClientMembershipEvent event);

}
