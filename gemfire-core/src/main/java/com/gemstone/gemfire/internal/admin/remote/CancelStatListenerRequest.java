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
   
   
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
//import com.gemstone.gemfire.*;
//import com.gemstone.gemfire.internal.*;
import java.io.*;
//import java.util.*;

/**
 * A message that is sent to a particular distribution manager to
 * get rid of a previously added statistic listener.
 */
public final class CancelStatListenerRequest extends AdminRequest {
  // instance variables
  private int listenerId;

  /**
   * Returns a <code>CancelStatListenerRequest</code> to be sent to the specified recipient.
   */
  public static CancelStatListenerRequest create(int listenerId) {
    CancelStatListenerRequest m = new CancelStatListenerRequest();
    m.listenerId = listenerId;
    return m;
  }

  public CancelStatListenerRequest() {
    friendlyName = LocalizedStrings.CancelStatListenerRequest_REMOVE_STATISTIC_RESOURCE_LISTENER.toLocalizedString();
  }

  /**
   * Must return a proper response to this request.
   */
  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    return CancelStatListenerResponse.create(dm, this.getSender(), this.listenerId); 
  }

  public int getDSFID() {
    return CANCEL_STAT_LISTENER_REQUEST;
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
    return LocalizedStrings.CancelStatListenerRequest_CANCELSTATLISTENERREQUEST_FROM_0_FOR_1.toLocalizedString
      (new Object[] {this.getRecipient(), Integer.valueOf(this.listenerId)});
  }
}
