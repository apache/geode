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
package org.apache.geode.distributed.internal.membership.gms;

import org.apache.geode.distributed.internal.membership.api.LifecycleListener;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;

/**
 * LifecycleListenerNoOp is the default implementation of LifecycleListener. Create and
 * install your own if you want to receive Membership lifecycle notifications.
 */
public class LifecycleListenerNoOp<ID extends MemberIdentifier> implements LifecycleListener<ID> {
  @Override
  public void start(final ID memberID) {

  }

  @Override
  public boolean disconnect(final Exception exception) {
    return false;
  }

  @Override
  public void joinCompleted(final ID address) {

  }

  @Override
  public void destroyMember(final ID member, final String reason) {

  }

  @Override
  public void forcedDisconnect(String reason, RECONNECTING isReconnect) {

  }
}
