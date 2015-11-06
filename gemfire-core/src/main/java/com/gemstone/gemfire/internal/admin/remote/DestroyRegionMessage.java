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
