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

//import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
//import com.gemstone.gemfire.internal.admin.*;
import com.gemstone.gemfire.distributed.internal.*;
import java.io.*;
//import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.*;
import com.gemstone.gemfire.internal.statistics.GemFireStatSampler;

/**
 * A message that is sent to a particular distribution manager to
 * get its current <code>RemoteCancelStatListener</code>.
 */
public final class CancelStatListenerResponse extends AdminResponse {
  // instance variables
  
  /**
   * Returns a <code>CancelStatListenerResponse</code> that will be returned to the
   * specified recipient. The message will contains a copy of the local manager's
   * system config.
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
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
  }

  @Override
  public String toString() {
    return LocalizedStrings.CancelStatListenerResponse_CANCELSTATLISTENERRESPONSE_FROM_0.toLocalizedString(this.getRecipient());
  }
}
