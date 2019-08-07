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
package org.apache.geode.distributed.internal.membership.gms.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.HighPriorityDistributionMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Version;

public class FinalCheckPassedMessage extends HighPriorityDistributionMessage {

  private InternalDistributedMember suspect;

  public FinalCheckPassedMessage() {}

  public FinalCheckPassedMessage(InternalDistributedMember recipient,
      InternalDistributedMember suspect) {
    super();
    setRecipient(recipient);
    this.suspect = suspect;
  }

  @Override
  public int getDSFID() {
    return FINAL_CHECK_PASSED_MESSAGE;
  }

  @Override
  public void process(ClusterDistributionManager dm) {
    throw new IllegalStateException("this message is not intended to execute in a thread pool");
  }

  @Override
  public String toString() {
    return "FinalCheckPassedMessage [suspect=" + suspect + "]";
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(suspect, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    suspect = (InternalDistributedMember) DataSerializer.readObject(in);
  }

  public InternalDistributedMember getSuspect() {
    return suspect;
  }
}
