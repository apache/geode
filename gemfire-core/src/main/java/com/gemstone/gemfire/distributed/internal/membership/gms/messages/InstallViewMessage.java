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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.internal.InternalDataSerializer;

public class InstallViewMessage extends HighPriorityDistributionMessage {

  enum messageType {
    INSTALL, PREPARE, SYNC
  }
  private NetView view;
  private Object credentials;
  private messageType kind;

  public InstallViewMessage(NetView view, Object credentials) {
    this.view = view;
    this.kind = messageType.INSTALL;
    this.credentials = credentials;
  }

  public InstallViewMessage(NetView view, Object credentials, boolean preparing) {
    this.view = view;
    this.kind = preparing? messageType.PREPARE : messageType.INSTALL;
    this.credentials = credentials;
  }
  
  public InstallViewMessage() {
    // no-arg constructor for serialization
  }
  
  public boolean isRebroadcast() {
    return kind == messageType.SYNC;
  }

  public NetView getView() {
    return view;
  }

  public Object getCredentials() {
    return credentials;
  }

  public boolean isPreparing() {
    return kind == messageType.PREPARE;
  }

  @Override
  public int getDSFID() {
    return INSTALL_VIEW_MESSAGE;
  }

  @Override
  protected void process(DistributionManager dm) {
    throw new IllegalStateException("this message is not intended to execute in a thread pool");
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(kind.ordinal());
    DataSerializer.writeObject(this.view, out);
    DataSerializer.writeObject(this.credentials, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.kind = messageType.values()[in.readInt()];
    this.view = DataSerializer.readObject(in);
    this.credentials = DataSerializer.readObject(in);
  }

  @Override
  public String toString() {
    return "InstallViewMessage(type="+this.kind+"; "+this.view
            +"; cred="+(credentials==null?"null": "not null")
             +")";
  }

}
