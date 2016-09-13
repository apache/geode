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
package org.apache.geode.internal.cache.tier.sockets.command;

import java.io.IOException;
import java.util.Set;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.FixedPartitionAttributes;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.internal.GetClientPartitionAttributesOp;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;

/**
 * {@link Command} for {@link GetClientPartitionAttributesOp} operation for 6.6
 * clients
 * @since GemFire 6.6
 */
public class GetClientPartitionAttributesCommand66 extends BaseCommand {

  private final static GetClientPartitionAttributesCommand66 singleton = new GetClientPartitionAttributesCommand66();

  public static Command getCommand() {
    return singleton;
  }

  GetClientPartitionAttributesCommand66() {
  }

  @SuppressWarnings("unchecked")
  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
    throws IOException, ClassNotFoundException, InterruptedException
  {
    String regionFullPath = null;
    regionFullPath = msg.getPart(0).getString();
    String errMessage = "";
    if (regionFullPath == null) {
      logger.warn(LocalizedMessage.create(LocalizedStrings.GetClientPartitionAttributes_THE_INPUT_REGION_PATH_IS_NULL));
      errMessage = LocalizedStrings.GetClientPartitionAttributes_THE_INPUT_REGION_PATH_IS_NULL
        .toLocalizedString();
      writeErrorResponse(msg,
        MessageType.GET_CLIENT_PARTITION_ATTRIBUTES_ERROR,
        errMessage.toString(), servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }
    Region region = servConn.getCache().getRegion(regionFullPath);
    if (region == null) {
      logger.warn(LocalizedMessage
        .create(LocalizedStrings.GetClientPartitionAttributes_REGION_NOT_FOUND_FOR_SPECIFIED_REGION_PATH,
          regionFullPath));
      errMessage = LocalizedStrings.GetClientPartitionAttributes_REGION_NOT_FOUND
        .toLocalizedString() + regionFullPath;
      writeErrorResponse(msg,
        MessageType.GET_CLIENT_PARTITION_ATTRIBUTES_ERROR,
        errMessage.toString(), servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    try {
      Message responseMsg = servConn.getResponseMessage();
      responseMsg.setTransactionId(msg.getTransactionId());
      responseMsg
        .setMessageType(MessageType.RESPONSE_CLIENT_PARTITION_ATTRIBUTES);

      if (!(region instanceof PartitionedRegion)) {
        responseMsg.setNumberOfParts(2);
        responseMsg.addObjPart(-1);
        responseMsg.addObjPart(region.getFullPath());
      }
      else {

        PartitionedRegion prRgion = (PartitionedRegion) region;

        PartitionResolver partitionResolver = prRgion
          .getPartitionResolver();
        int numParts = 2; // MINUMUM PARTS
        if (partitionResolver != null) {
          numParts++;
        }
        if (prRgion.isFixedPartitionedRegion()) {
          numParts++;
        }
        responseMsg.setNumberOfParts(numParts);
        // PART 1
        responseMsg.addObjPart(prRgion.getTotalNumberOfBuckets());

        // PART 2
        String leaderRegionPath = null;
        PartitionedRegion leaderRegion = null;
        String leaderRegionName = prRgion.getColocatedWith();
        if (leaderRegionName != null) {
          Cache cache = prRgion.getCache();
          while (leaderRegionName != null) {
            leaderRegion = (PartitionedRegion) cache
              .getRegion(leaderRegionName);
            if (leaderRegion.getColocatedWith() == null) {
              leaderRegionPath = leaderRegion.getFullPath();
              break;
            }
            else {
              leaderRegionName = leaderRegion.getColocatedWith();
            }
          }
        }
        responseMsg.addObjPart(leaderRegionPath);

        // PART 3
        if (partitionResolver != null) {
          responseMsg.addObjPart(partitionResolver.getClass().toString()
            .substring(6));
        }
        // PART 4
        if (prRgion.isFixedPartitionedRegion()) {
          Set<FixedPartitionAttributes> fpaSet = null;
          if (leaderRegion != null) {
            fpaSet = PartitionedRegionHelper
              .getAllFixedPartitionAttributes(leaderRegion);
          }
          else {
            fpaSet = PartitionedRegionHelper
              .getAllFixedPartitionAttributes(prRgion);
          }
          responseMsg.addObjPart(fpaSet);
        }
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


