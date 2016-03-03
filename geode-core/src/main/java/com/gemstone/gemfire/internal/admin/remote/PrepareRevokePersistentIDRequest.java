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
import java.util.Set;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.persistence.RevokeFailedException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberManager;
import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberPattern;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * An instruction to all members that they should forget 
 * about the persistent member described by this pattern.
 * TODO prpersist - This extends AdminRequest, but it doesn't
 * work with most of the admin paradigm, which is a request response
 * to a single member. Maybe we need to a new base class.
 *
 */
public class PrepareRevokePersistentIDRequest extends CliLegacyMessage {
  PersistentMemberPattern pattern;
  private boolean cancel;
  
  public PrepareRevokePersistentIDRequest() {
    
  }
  
  public PrepareRevokePersistentIDRequest(PersistentMemberPattern pattern, boolean cancel) {
    this.pattern = pattern;
    this.cancel = cancel;
  }
  
  public static void cancel(DM dm, PersistentMemberPattern pattern) {
    send(dm, pattern, true);
  }
  
  public static void send(DM dm, PersistentMemberPattern pattern) {
    send(dm, pattern, false);
  }

  private static void send(DM dm, PersistentMemberPattern pattern, boolean cancel) {
    Set recipients = dm.getOtherDistributionManagerIds();
    recipients.remove(dm.getId());
    PrepareRevokePersistentIDRequest request = new PrepareRevokePersistentIDRequest(pattern, cancel);
    request.setRecipients(recipients);
    
    AdminMultipleReplyProcessor replyProcessor = new AdminMultipleReplyProcessor(dm, recipients);
    request.msgId = replyProcessor.getProcessorId();
    dm.putOutgoing(request);
    try {
      replyProcessor.waitForReplies();
    } catch (ReplyException e) {
      if(e.getCause() instanceof CancelException) {
        //ignore
        return;
      }
      throw e;
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    request.setSender(dm.getId());
    request.createResponse((DistributionManager)dm);
  }
  
  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if(cache != null && !cache.isClosed()) {
      PersistentMemberManager mm = cache.getPersistentMemberManager();
      if(cancel) {
        mm.cancelRevoke(pattern);
      } else {
        if(!mm.prepareRevoke(pattern, dm, getSender())) {
          throw new RevokeFailedException(
              LocalizedStrings.RevokeFailedException_Member_0_is_already_running_1
                  .toLocalizedString(dm.getId(), pattern));
        }
      }
    }

    return new RevokePersistentIDResponse(this.getSender());
  }

  public int getDSFID() {
    return PREPARE_REVOKE_PERSISTENT_ID_REQUEST;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    pattern = new PersistentMemberPattern();
    InternalDataSerializer.invokeFromData(pattern, in);
    cancel = in.readBoolean();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    InternalDataSerializer.invokeToData(pattern, out);
    out.writeBoolean(cancel);
  }

}
