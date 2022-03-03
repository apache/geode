/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.persistence.PersistentMemberManager;
import org.apache.geode.internal.cache.persistence.PersistentMemberPattern;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * An instruction to all members that they should forget about the persistent member described by
 * this pattern. TODO prpersist - This extends AdminRequest, but it doesn't work with most of the
 * admin paradigm, which is a request response to a single member. Maybe we need to a new base
 * class.
 */
public class RevokePersistentIDRequest extends CliLegacyMessage {
  private static final Logger logger = LogService.getLogger();

  private PersistentMemberPattern pattern;

  public RevokePersistentIDRequest() {
    // do nothing
  }

  public RevokePersistentIDRequest(PersistentMemberPattern pattern) {
    this.pattern = pattern;
  }

  public static void send(DistributionManager dm, PersistentMemberPattern pattern) {
    Set recipients = dm.getOtherDistributionManagerIds();
    RevokePersistentIDRequest request = new RevokePersistentIDRequest(pattern);
    request.setRecipients(recipients);

    AdminMultipleReplyProcessor replyProcessor = new AdminMultipleReplyProcessor(dm, recipients);
    request.msgId = replyProcessor.getProcessorId();
    dm.putOutgoing(request);
    try {
      replyProcessor.waitForReplies();
    } catch (ReplyException e) {
      if (e.getCause() instanceof CancelException) {
        // ignore
        return;
      }
      throw e;
    } catch (InterruptedException e) {
      logger.warn(e);
    }
    request.createResponse(dm);
  }

  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    InternalCache cache = dm.getCache();
    if (cache != null && !cache.isClosed()) {
      PersistentMemberManager mm = cache.getPersistentMemberManager();
      mm.revokeMember(pattern);
    }

    return new RevokePersistentIDResponse(getSender());
  }

  @Override
  public int getDSFID() {
    return REVOKE_PERSISTENT_ID_REQUEST;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    pattern = new PersistentMemberPattern();
    InternalDataSerializer.invokeFromData(pattern, in);
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    InternalDataSerializer.invokeToData(pattern, out);
  }
}
