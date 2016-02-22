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

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;


/**
 * An extension of AdminRequest for messages that are used
 * as part of the new CLI. The new CLI expects errors to be logged on
 * the side where he message is processed, which none of the rest of
 * gemfire messages do. This is a extension of AdminRequest so that old
 * admin messages which are still used as part of the new CLI still log the
 * message.
 *
 */
public abstract class CliLegacyMessage extends AdminRequest {
  private static final Logger logger = LogService.getLogger();

  @Override
  protected void process(DistributionManager dm) {
    AdminResponse response = null;
    try {
      response = createResponse(dm);
    } catch (Exception ex) {
      logger.error(LocalizedMessage.create(LocalizedStrings.CliLegacyMessage_ERROR, this.getClass()), ex);
      response = AdminFailureResponse.create(dm, this.getSender(), ex);
      
    }
    if (response != null) { //cancellations result in null response
      response.setMsgId(this.getMsgId());
      dm.putOutgoing(response);
    } else {
      logger.info(LocalizedMessage.create(LocalizedStrings.AdminRequest_RESPONSE_TO__0__WAS_CANCELLED, this.getClass().getName()));
    }
  }

}
