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
package org.apache.geode.internal.admin.remote;

import java.util.Date;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.Alert;
import org.apache.geode.internal.admin.GemFireVM;

public class VersionMismatchAlert implements Alert {
  private final RemoteGfManagerAgent source;
  private final String sourceId;
  private final Date time;
  private final String message;
  private final InternalDistributedMember sender;

  public VersionMismatchAlert(RemoteGfManagerAgent sender, String message) {
    source = sender;
    sourceId = sender.toString();
    time = new Date(System.currentTimeMillis());
    this.message = message;
    /* sender in this case is going to be the agent itself. */
    if (sender.getDM() != null) {
      this.sender = sender.getDM().getId();
    } else {
      this.sender = null;
    }
  }

  @Override
  public int getLevel() {
    return Alert.SEVERE;
  }

  @Override
  public GemFireVM getGemFireVM() {
    return null;
  }

  @Override
  public String getConnectionName() {
    return null;
  }

  @Override
  public String getSourceId() {
    return sourceId;
  }

  @Override
  public String getMessage() {
    return message;
  }

  @Override
  public java.util.Date getDate() {
    return time;
  }

  public RemoteGfManagerAgent getManagerAgent() {
    return source;
  }

  /**
   * Returns a InternalDistributedMember instance representing the agent.
   *
   * @return the InternalDistributedMember instance representing this agent instance
   *
   * @since GemFire 6.5
   */
  @Override
  public InternalDistributedMember getSender() {
    return sender;
  }

}
