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
//import com.gemstone.gemfire.internal.*;
//import com.gemstone.gemfire.internal.admin.*;
import com.gemstone.gemfire.distributed.internal.*;
import java.io.*;
//import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * The response to adding a health listener.
 * @since GemFire 3.5
 */
public final class RemoveHealthListenerResponse extends AdminResponse {
  // instance variables
  
  /**
   * Returns a <code>RemoveHealthListenerResponse</code> that will be returned to the
   * specified recipient.
   */
  public static RemoveHealthListenerResponse create(DistributionManager dm, InternalDistributedMember recipient, int id) {
    RemoveHealthListenerResponse m = new RemoveHealthListenerResponse();
    m.setRecipient(recipient);
    dm.removeHealthMonitor(recipient, id);
    return m;
  }

  // instance methods
  public int getDSFID() {
    return REMOVE_HEALTH_LISTENER_RESPONSE;
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
    return "RemoveHealthListenerResponse from " + this.getRecipient();
  }
}
