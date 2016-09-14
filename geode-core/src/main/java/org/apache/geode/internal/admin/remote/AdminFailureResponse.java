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

import org.apache.geode.distributed.internal.*;
import org.apache.geode.*;
//import org.apache.geode.internal.*;
import java.io.*;
//import java.util.*;
import org.apache.geode.distributed.internal.membership.*;

/**
 * A response to a failed request.
 */
public final class AdminFailureResponse extends AdminResponse {
  // instance variables
  Exception cause;
  
  /**
   * Returns a <code>AdminFailureResponse</code> that will be returned to the
   * specified recipient. The message will contains a copy of the local manager's
   * system config.
   */
  public static AdminFailureResponse create(DistributionManager dm, InternalDistributedMember recipient, Exception cause) {
    AdminFailureResponse m = new AdminFailureResponse();
    m.setRecipient(recipient);
    m.cause = cause;
    return m;
  }

  // instance methods
  public Exception getCause() {
    return this.cause;
  }
  
  public int getDSFID() {
    return ADMIN_FAILURE_RESPONSE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.cause, out);
  }

  @Override
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.cause = (Exception)DataSerializer.readObject(in);
  }

  @Override
  public String toString() {
    return "AdminFailureResponse from " + this.getRecipient() + " cause=" + this.cause;
  }
}
