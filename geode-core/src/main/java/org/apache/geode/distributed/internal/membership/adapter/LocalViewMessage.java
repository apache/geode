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
package org.apache.geode.distributed.internal.membership.adapter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.ClusterOperationExecutors;
import org.apache.geode.distributed.internal.SerialDistributionMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.MembershipView;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;


/**
 * LocalViewMessage is used to pass a new membership view to the GemFire cache in an orderly manner.
 * It is intended to be queued with serially executed messages so that the view takes effect at the
 * proper time.
 *
 */

public class LocalViewMessage extends SerialDistributionMessage {

  private GMSMembershipManager manager;
  private long viewId;
  private MembershipView view;

  public LocalViewMessage(InternalDistributedMember addr, long viewId, MembershipView view,
      GMSMembershipManager manager) {
    super();
    this.sender = addr;
    this.viewId = viewId;
    this.view = view;
    this.manager = manager;
  }

  @Override
  public int getProcessorType() {
    return ClusterOperationExecutors.VIEW_EXECUTOR;
  }


  @Override
  protected void process(ClusterDistributionManager dm) {
    // dm.getLogger().info("view message processed", new Exception());
    manager.processView(viewId, view);
  }

  // These "messages" are never DataSerialized

  @Override
  public int getDSFID() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    throw new UnsupportedOperationException();
  }
}
