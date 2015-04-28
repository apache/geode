/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.*;
import java.io.*;
//import java.net.*;
//import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * A message that is sent in response to a {@link VersionInfoRequest}.
 * @since 3.5
 */
public final class VersionInfoResponse extends AdminResponse {
  // instance variables
  private String verInfo;
  
  
  /**
   * Returns a <code>VersionInfoResponse</code> that will be returned to the
   * specified recipient.
   */
  public static VersionInfoResponse create(DistributionManager dm, InternalDistributedMember recipient) {
    VersionInfoResponse m = new VersionInfoResponse();
    m.setRecipient(recipient);
    m.verInfo = GemFireVersion.asString();
    return m;
  }

  public String getVersionInfo() {
    return this.verInfo;
  }
  
  public int getDSFID() {
    return VERSION_INFO_RESPONSE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeString(this.verInfo, out);
  }

  @Override
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);    
    this.verInfo = DataSerializer.readString(in);
  }

  @Override
  public String toString() {
    return "VersionInfoResponse from " + this.getSender();
  }
}
