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
package org.apache.geode.distributed.internal.membership.api;


/**
 * You may supply a LifecycleListener when building a Membership. Your listener
 * will be invoked at various points in the lifecycle of the Membership.
 *
 * @param <ID>
 */
public interface LifecycleListener<ID extends MemberIdentifier> {

  enum RECONNECTING {
    RECONNECTING, NOT_RECONNECTING
  }

  /**
   * Invoked when the Membership is starting. All membership services will have been
   * initialized and had their "start" methods invoked but we will not yet have joined the cluster.
   *
   * @param memberID the initial membership identifier, which will not yet have a view ID
   */
  void start(
      final ID memberID);

  /**
   * Invoked when the Membership has successfully joined the cluster. At this point the
   * membership address is stable.
   */
  void joinCompleted(ID address);

  /**
   * Invoked when Membership is shutting down
   *
   * @param exception if not null, this is the reason for Membership shutdown
   */
  boolean disconnect(Exception exception);

  /**
   * Invoked when a member of the cluster has left and resources associated with that member
   * should be released.
   */
  void destroyMember(ID member, String reason);

  /**
   * Invoked if Membership has been forced out of the cluster.
   */
  void forcedDisconnect(String reason, RECONNECTING isReconnect);
}
