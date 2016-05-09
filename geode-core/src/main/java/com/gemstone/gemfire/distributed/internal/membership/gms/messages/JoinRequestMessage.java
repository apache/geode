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
package com.gemstone.gemfire.distributed.internal.membership.gms.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Version;

public class JoinRequestMessage extends HighPriorityDistributionMessage {
  private InternalDistributedMember memberID;
  private Object credentials;
  private int failureDetectionPort = -1;
  private Object publicKey;
  
  public JoinRequestMessage(InternalDistributedMember coord,
                            InternalDistributedMember id, Object credentials, int fdPort) {
    super();
    setRecipient(coord);
    this.memberID = id;
    this.credentials = credentials;
    this.publicKey = null;
    this.failureDetectionPort = fdPort;
  }
  public JoinRequestMessage() {
    // no-arg constructor for serialization
  }

  public void setPublicKey(Object key) {
    this.publicKey = key;
  }

  @Override
  public int getDSFID() {
    return JOIN_REQUEST;
  }
  
  @Override
  public void process(DistributionManager dm) {
    throw new IllegalStateException("this message is not intended to execute in a thread pool"); 
  }

  public InternalDistributedMember getMemberID() {
    return memberID;
  }

  public Object getCredentials() {
    return credentials;
  }

  public Object getPublicKey() {
    return publicKey;
  }
  
  @Override
  public String toString() {
    return getShortClassName() + "(" + memberID + (credentials==null? ")" : "; with credentials)") + " failureDetectionPort:" + failureDetectionPort;
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(memberID, out);
    DataSerializer.writeObject(credentials, out);
    DataSerializer.writeObject(publicKey, out);
    DataSerializer.writePrimitiveInt(failureDetectionPort, out);
    // preserve the multicast setting so the receiver can tell
    // if this is a mcast join request
    out.writeBoolean(getMulticast());
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    memberID = DataSerializer.readObject(in);
    credentials = DataSerializer.readObject(in);
    publicKey = DataSerializer.readObject(in);
    failureDetectionPort = DataSerializer.readPrimitiveInt(in);
    setMulticast(in.readBoolean());
  }

  public int getFailureDetectionPort() {
    return failureDetectionPort;
  }

}
