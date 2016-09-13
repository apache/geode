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

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.*;
import java.util.*;

public class VersionMismatchAlert implements Alert {
  private final RemoteGfManagerAgent source;
  private final String sourceId;
  private final Date time;
  private final String message;
  private final InternalDistributedMember sender;

  public VersionMismatchAlert(RemoteGfManagerAgent sender, String message) {
    this.source = sender;
    this.sourceId = sender.toString();
    this.time = new Date(System.currentTimeMillis());
    this.message = message;
    /* sender in this case is going to be the agent itself. */
    if (sender.getDM() != null) {
      this.sender = sender.getDM().getId();
    } else {
      this.sender = null;
    }
  }
  
  public int getLevel(){ return Alert.SEVERE; }
  public GemFireVM getGemFireVM() { return null; }
  public String getConnectionName(){ return null; }
  public String getSourceId(){ return this.sourceId; }
  public String getMessage(){ return this.message; }
  public java.util.Date getDate(){ return this.time; }

  public RemoteGfManagerAgent getManagerAgent(){
    return this.source;
  }

  /**
   * Returns a InternalDistributedMember instance representing the agent.
   * 
   * @return the InternalDistributedMember instance representing this agent
   *         instance
   *         
   * @since GemFire 6.5
   */
  public InternalDistributedMember getSender() {
    return this.sender;
  }

}
