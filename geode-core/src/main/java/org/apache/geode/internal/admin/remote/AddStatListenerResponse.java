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
   
   
package org.apache.geode.internal.admin.remote;

//import org.apache.geode.*;
//import org.apache.geode.internal.admin.*;
import org.apache.geode.distributed.internal.*;
import java.io.*;
//import java.util.*;
import org.apache.geode.distributed.internal.membership.*;
import org.apache.geode.internal.statistics.GemFireStatSampler;

/**
 * A message that is sent to a particular distribution manager to
 * get its current <code>RemoteAddStatListener</code>.
 */
public final class AddStatListenerResponse extends AdminResponse {
  // instance variables
  int listenerId;
  
  /**
   * Returns a <code>AddStatListenerResponse</code> that will be
   * returned to the specified recipient. The message will contains a
   * copy of the local manager's system config.
   */
  public static AddStatListenerResponse create(DistributionManager dm, InternalDistributedMember recipient, long resourceId, String statName) {
    AddStatListenerResponse m = new AddStatListenerResponse();
    m.setRecipient(recipient);
    GemFireStatSampler sampler = null;
    sampler = dm.getSystem().getStatSampler();
    if (sampler != null) {
      m.listenerId = sampler.addListener(recipient, resourceId, statName);
    }
    return m;
  }

  // instance methods
  public int getListenerId() {
    return this.listenerId;
  }
  
  public int getDSFID() {
    return ADD_STAT_LISTENER_RESPONSE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.listenerId);
  }

  @Override
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.listenerId = in.readInt();
  }

  @Override
  public String toString() {
    return "AddStatListenerResponse from " + this.getRecipient() + " listenerId=" + this.listenerId;
  }
}
