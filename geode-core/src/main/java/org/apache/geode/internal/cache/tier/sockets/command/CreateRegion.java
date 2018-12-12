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
package org.apache.geode.internal.cache.tier.sockets.command;

import java.io.IOException;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public class CreateRegion extends BaseCommand {

  private static final CreateRegion singleton = new CreateRegion();

  public static Command getCommand() {
    return singleton;
  }

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start) throws IOException {
    Part regionNamePart = null;
    String regionName = null;
    serverConnection.setAsTrue(REQUIRES_RESPONSE);
    // bserverStats.incLong(readDestroyRequestTimeId,
    // DistributionStats.getStatTime() - start);
    // bserverStats.incInt(destroyRequestsId, 1);
    // start = DistributionStats.getStatTime();
    // Retrieve the data from the message parts
    Part parentRegionNamePart = clientMessage.getPart(0);
    String parentRegionName = parentRegionNamePart.getString();

    regionNamePart = clientMessage.getPart(1);
    regionName = regionNamePart.getString();

    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: Received create region request ({} bytes) from {} for parent region {} region {}",
          serverConnection.getName(), clientMessage.getPayloadLength(),
          serverConnection.getSocketString(), parentRegionName, regionName);
    }

    // Process the create region request
    if (parentRegionName == null || regionName == null) {
      String errMessage = "";
      if (parentRegionName == null) {
        logger.warn("{}: The input parent region name for the create region request is null",
            serverConnection.getName());
        errMessage =
            "The input parent region name for the create region request is null";
      }
      if (regionName == null) {
        logger.warn("{}: The input region name for the create region request is null",
            serverConnection.getName());
        errMessage =
            "The input region name for the create region request is null";
      }
      writeErrorResponse(clientMessage, MessageType.CREATE_REGION_DATA_ERROR, errMessage,
          serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    Region parentRegion = serverConnection.getCache().getRegion(parentRegionName);
    if (parentRegion == null) {
      String reason =
          String.format("%s was not found during subregion creation request",
              parentRegionName);
      writeRegionDestroyedEx(clientMessage, parentRegionName, reason, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    try {
      securityService.authorize(Resource.DATA, Operation.MANAGE);
    } catch (NotAuthorizedException ex) {
      writeException(clientMessage, ex, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    AuthorizeRequest authzRequest = serverConnection.getAuthzRequest();
    if (authzRequest != null) {
      try {
        authzRequest.createRegionAuthorize(parentRegionName + '/' + regionName);
      } catch (NotAuthorizedException ex) {
        writeException(clientMessage, ex, false, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
        return;
      }
    }
    // Create or get the subregion
    Region region = parentRegion.getSubregion(regionName);
    if (region == null) {
      AttributesFactory factory = new AttributesFactory(parentRegion.getAttributes());
      region = parentRegion.createSubregion(regionName, factory.create());
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Created region {}", serverConnection.getName(), region);
      }
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Retrieved region {}", serverConnection.getName(), region);
      }
    }

    // Update the statistics and write the reply
    // start = DistributionStats.getStatTime(); WHY ARE WE GETTING START AND
    // NOT USING IT
    // bserverStats.incLong(processDestroyTimeId,
    // DistributionStats.getStatTime() - start);
    writeReply(clientMessage, serverConnection);
    serverConnection.setAsTrue(RESPONDED);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sent create region response for parent region {} region {}",
          serverConnection.getName(), parentRegionName, regionName);
    }
  }

}
