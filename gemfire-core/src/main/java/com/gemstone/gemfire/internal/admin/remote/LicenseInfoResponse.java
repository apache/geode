/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;

//import java.net.*;

/**
 * A message that is sent in response to a {@link LicenseInfoRequest}.
 */
public final class LicenseInfoResponse extends AdminResponse {
  private static final Logger logger = LogService.getLogger();
  
  // instance variables
  private Properties p;
  
  
  /**
   * Returns a <code>LicenseInfoResponse</code> that will be returned to the
   * specified recipient.
   */
  public static LicenseInfoResponse create(DistributionManager dm, InternalDistributedMember recipient) {
    LicenseInfoResponse m = new LicenseInfoResponse();
    m.setRecipient(recipient);
    m.p = new Properties();

    return m;
  }

  public Properties getLicenseInfo() {
    return p;
  }
  
  public int getDSFID() {
    return LICENSE_INFO_RESPONSE;
  }

  @Override  
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.p, out);
  }

  @Override  
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);    
    this.p = (Properties)DataSerializer.readObject(in);
  }

  @Override  
  public String toString() {
    return "LicenseInfoResponse from " + this.getSender();
  }
}
