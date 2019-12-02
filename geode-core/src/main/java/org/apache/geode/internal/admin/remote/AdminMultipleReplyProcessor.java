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

import java.util.Collection;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyProcessor21;

/**
 * TODO prpersist. This code really needs to be merged with the AdminReplyProcessor. However, we're
 * getting close to the release and I don't want to mess with all of the admin code right now. We
 * need this class to handle failures from admin messages that expect replies from multiple members.
 */
public class AdminMultipleReplyProcessor extends ReplyProcessor21 {

  public AdminMultipleReplyProcessor(DistributionManager dm, Collection initMembers) {
    super(dm, initMembers);
  }

  @Override
  protected void process(DistributionMessage message, boolean warn) {
    if (message instanceof AdminFailureResponse) {
      Exception ex = ((AdminFailureResponse) message).getCause();
      if (ex != null) {
        ReplyException replyException = new ReplyException(ex);
        replyException.setSenderIfNull(message.getSender());
        processException(message, replyException);
      }
    }
    super.process(message, warn);
  }
}
