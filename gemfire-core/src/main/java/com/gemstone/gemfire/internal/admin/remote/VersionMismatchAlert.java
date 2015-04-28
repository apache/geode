/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.admin.*;
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
   * @since 6.5
   */
  public InternalDistributedMember getSender() {
    return this.sender;
  }

}
