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
/**
 *
 */
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import java.io.IOException;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;
import com.gemstone.gemfire.security.NotAuthorizedException;

public class CreateRegion extends BaseCommand {

  private final static CreateRegion singleton = new CreateRegion();

  public static Command getCommand() {
    return singleton;
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start) throws IOException {
    Part regionNamePart = null;
    String regionName = null;
    servConn.setAsTrue(REQUIRES_RESPONSE);
    // bserverStats.incLong(readDestroyRequestTimeId,
    // DistributionStats.getStatTime() - start);
    // bserverStats.incInt(destroyRequestsId, 1);
    //    start = DistributionStats.getStatTime();
    // Retrieve the data from the message parts
    Part parentRegionNamePart = msg.getPart(0);
    String parentRegionName = parentRegionNamePart.getString();

    regionNamePart = msg.getPart(1);
    regionName = regionNamePart.getString();

    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received create region request ({} bytes) from {} for parent region {} region {}", servConn.getName(), msg
        .getPayloadLength(), servConn.getSocketString(), parentRegionName, regionName);
    }

    // Process the create region request
    if (parentRegionName == null || regionName == null) {
      String errMessage = "";
      if (parentRegionName == null) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.CreateRegion_0_THE_INPUT_PARENT_REGION_NAME_FOR_THE_CREATE_REGION_REQUEST_IS_NULL, servConn
          .getName()));
        errMessage = LocalizedStrings.CreateRegion_THE_INPUT_PARENT_REGION_NAME_FOR_THE_CREATE_REGION_REQUEST_IS_NULL.toLocalizedString();
      }
      if (regionName == null) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.CreateRegion_0_THE_INPUT_REGION_NAME_FOR_THE_CREATE_REGION_REQUEST_IS_NULL, servConn
          .getName()));
        errMessage = LocalizedStrings.CreateRegion_THE_INPUT_REGION_NAME_FOR_THE_CREATE_REGION_REQUEST_IS_NULL.toLocalizedString();
      }
      writeErrorResponse(msg, MessageType.CREATE_REGION_DATA_ERROR, errMessage, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    Region parentRegion = servConn.getCache().getRegion(parentRegionName);
    if (parentRegion == null) {
      String reason = LocalizedStrings.CreateRegion__0_WAS_NOT_FOUND_DURING_SUBREGION_CREATION_REQUEST.toLocalizedString(parentRegionName);
      writeRegionDestroyedEx(msg, parentRegionName, reason, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    try {
      this.securityService.authorizeDataManage();
    } catch (NotAuthorizedException ex) {
      writeException(msg, ex, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    AuthorizeRequest authzRequest = servConn.getAuthzRequest();
    if (authzRequest != null) {
      try {
        authzRequest.createRegionAuthorize(parentRegionName + '/' + regionName);
      } catch (NotAuthorizedException ex) {
        writeException(msg, ex, false, servConn);
        servConn.setAsTrue(RESPONDED);
        return;
      }
    }
    // Create or get the subregion
    Region region = parentRegion.getSubregion(regionName);
    if (region == null) {
      AttributesFactory factory = new AttributesFactory(parentRegion.getAttributes());
      region = parentRegion.createSubregion(regionName, factory.create());
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Created region {}", servConn.getName(), region);
      }
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Retrieved region {}", servConn.getName(), region);
      }
    }

    // Update the statistics and write the reply
    // start = DistributionStats.getStatTime(); WHY ARE WE GETTING START AND
    // NOT USING IT
    // bserverStats.incLong(processDestroyTimeId,
    // DistributionStats.getStatTime() - start);
    writeReply(msg, servConn);
    servConn.setAsTrue(RESPONDED);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sent create region response for parent region {} region {}", servConn.getName(), parentRegionName, regionName);
    }
  }

}
