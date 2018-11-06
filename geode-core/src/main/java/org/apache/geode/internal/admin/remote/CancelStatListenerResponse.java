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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.statistics.GemFireStatSampler;

/**
 * A message that is sent to a particular distribution manager to get its current
 * <code>RemoteCancelStatListener</code>.
 */
public class CancelStatListenerResponse extends AdminResponse {
  // instance variables

  /**
   * Returns a <code>CancelStatListenerResponse</code> that will be returned to the specified
   * recipient. The message will contains a copy of the local manager's system config.
   */
  public static CancelStatListenerResponse create(DistributionManager dm,
      InternalDistributedMember recipient, int listenerId) {
    CancelStatListenerResponse m = new CancelStatListenerResponse();
    m.setRecipient(recipient);
    GemFireStatSampler sampler = null;
    sampler = dm.getSystem().getStatSampler();
    if (sampler != null) {
      sampler.removeListener(listenerId);
    }
    return m;
  }

  // instance methods
  public int getDSFID() {
    return CANCEL_STAT_LISTENER_RESPONSE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
  }

  @Override
  public String toString() {
    return String.format("CancelStatListenerResponse from %s",
        this.getRecipient());
  }
}
