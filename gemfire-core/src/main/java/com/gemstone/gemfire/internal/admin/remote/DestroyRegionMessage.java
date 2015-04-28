/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */   
   
package com.gemstone.gemfire.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.ExpirationAction;
//import com.gemstone.gemfire.internal.*;
//import com.gemstone.gemfire.internal.admin.*;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
//import java.util.*;

/**
 * A message that is sent to a particular distribution manager to let
 * it know that the sender is an administation console that just connected.
 */
public final class DestroyRegionMessage extends RegionAdminMessage {
  
  private static final Logger logger = LogService.getLogger();
  
  private ExpirationAction action;

  public static DestroyRegionMessage create(ExpirationAction action) {
    DestroyRegionMessage m = new DestroyRegionMessage();
    m.action = action;
    return m;
  }

  @Override
  public void process(DistributionManager dm) {
    Region r = getRegion(dm.getSystem());
    if (r != null) {
      try {
        if (action == ExpirationAction.LOCAL_DESTROY) {
          r.localDestroyRegion();
        } else if (action == ExpirationAction.DESTROY) {
          r.destroyRegion();
        } else if (action == ExpirationAction.INVALIDATE) {
          r.invalidateRegion();
        } else if (action == ExpirationAction.LOCAL_INVALIDATE) {
          r.localInvalidateRegion();
        }
      } catch (Exception e) {
        logger.warn(LocalizedMessage.create(
            LocalizedStrings.DestroRegionMessage_FAILED_ATTEMPT_TO_DESTROY_OR_INVALIDATE_REGION_0_FROM_CONSOLE_AT_1,
            new Object[] {r.getFullPath(), this.getSender()}));
      }
    }
  }

  public int getDSFID() {
    return ADMIN_DESTROY_REGION_MESSAGE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.action, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    super.fromData(in);
    this.action = (ExpirationAction)DataSerializer.readObject(in);
  }

  @Override
  public String toString(){
    return LocalizedStrings.DestroyRegionMessage_DESTROYREGIONMESSAGE_FROM_0.toLocalizedString(this.getSender());
  }
}
