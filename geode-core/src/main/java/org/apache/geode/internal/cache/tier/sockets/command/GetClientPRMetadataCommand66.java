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
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.BucketServerLocation66;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
/**
 * {@link Command} for {@link GetClientPRMetadataCommand66}
 * 
 * 
 * @since GemFire 6.6
 */
public class GetClientPRMetadataCommand66 extends BaseCommand {

  private final static GetClientPRMetadataCommand66 singleton = new GetClientPRMetadataCommand66();

  public static Command getCommand() {
    return singleton;
  }

  private GetClientPRMetadataCommand66() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException, ClassNotFoundException, InterruptedException {
    String regionFullPath = null;
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    regionFullPath = msg.getPart(0).getString();
    String errMessage = "";
    if (regionFullPath == null) {
      logger.warn(LocalizedMessage.create(LocalizedStrings.GetClientPRMetadata_THE_INPUT_REGION_PATH_IS_NULL));
      errMessage = LocalizedStrings.GetClientPRMetadata_THE_INPUT_REGION_PATH_IS_NULL
          .toLocalizedString();
      writeErrorResponse(msg, MessageType.GET_CLIENT_PR_METADATA_ERROR,
          errMessage.toString(), servConn);
      servConn.setAsTrue(RESPONDED);
    }
    else {
      Region region = crHelper.getRegion(regionFullPath);
      if (region == null) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.GetClientPRMetadata_REGION_NOT_FOUND_FOR_SPECIFIED_REGION_PATH, regionFullPath));
        errMessage = LocalizedStrings.GetClientPRMetadata_REGION_NOT_FOUND
            .toLocalizedString()
            + regionFullPath;
        writeErrorResponse(msg, MessageType.GET_CLIENT_PR_METADATA_ERROR,
            errMessage.toString(), servConn);
        servConn.setAsTrue(RESPONDED);
      }
      else {
        try {
          Message responseMsg = servConn.getResponseMessage();
          responseMsg.setTransactionId(msg.getTransactionId());
          responseMsg.setMessageType(MessageType.RESPONSE_CLIENT_PR_METADATA);

          PartitionedRegion prRgion = (PartitionedRegion)region;
          Map<Integer, List<BucketServerLocation66>> bucketToServerLocations = prRgion
              .getRegionAdvisor().getAllClientBucketProfiles();
          responseMsg.setNumberOfParts(bucketToServerLocations.size());
          for (List<BucketServerLocation66> serverLocations : bucketToServerLocations
              .values()) {
            responseMsg.addObjPart(serverLocations);
          }
          responseMsg.send();
          msg.clearParts();
        }
        catch (Exception e) {
          writeException(msg, e, false, servConn);
        }
        finally {
          servConn.setAsTrue(Command.RESPONDED);
        }
      }
    }
  }

}
