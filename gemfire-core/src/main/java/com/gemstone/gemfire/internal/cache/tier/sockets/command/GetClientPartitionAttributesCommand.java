/*
 * ========================================================================= 
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import java.io.IOException;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.FixedPartitionAttributes;
import com.gemstone.gemfire.cache.PartitionResolver;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.internal.GetClientPartitionAttributesOp;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
/**
 * {@link Command} for {@link GetClientPartitionAttributesOp} operation 
 * 
 * @author Suranjan Kumar
 * @author Yogesh Mahajan
 * 
 * @since 6.5
 *
 */
public class GetClientPartitionAttributesCommand extends BaseCommand {

  private final static GetClientPartitionAttributesCommand singleton = new GetClientPartitionAttributesCommand();

  public static Command getCommand() {
    return singleton;
  }

  private GetClientPartitionAttributesCommand() {
  }

  @SuppressWarnings("unchecked")
  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException, ClassNotFoundException, InterruptedException {
    String regionFullPath = null;
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    regionFullPath = msg.getPart(0).getString();
    String errMessage = "";
    if (regionFullPath == null) {
      logger.warn(LocalizedMessage.create(LocalizedStrings.GetClientPartitionAttributes_THE_INPUT_REGION_PATH_IS_NULL));
      errMessage = LocalizedStrings.GetClientPartitionAttributes_THE_INPUT_REGION_PATH_IS_NULL
          .toLocalizedString();
      writeErrorResponse(msg,
          MessageType.GET_CLIENT_PARTITION_ATTRIBUTES_ERROR, errMessage
              .toString(), servConn);
      servConn.setAsTrue(RESPONDED);
    }
    else {
      Region region = crHelper.getRegion(regionFullPath);
      if (region == null) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.GetClientPartitionAttributes_REGION_NOT_FOUND_FOR_SPECIFIED_REGION_PATH, regionFullPath));
        errMessage = LocalizedStrings.GetClientPartitionAttributes_REGION_NOT_FOUND
            .toLocalizedString()
            + regionFullPath;
        writeErrorResponse(msg,
            MessageType.GET_CLIENT_PARTITION_ATTRIBUTES_ERROR, errMessage
                .toString(), servConn);
        servConn.setAsTrue(RESPONDED);
      }
      else {
        try {
          Message responseMsg = servConn.getResponseMessage();
          responseMsg.setTransactionId(msg.getTransactionId());
          responseMsg
              .setMessageType(MessageType.RESPONSE_CLIENT_PARTITION_ATTRIBUTES);

          PartitionedRegion prRgion = (PartitionedRegion)region;

          PartitionResolver partitionResolver = prRgion.getPartitionResolver();
          int numParts = 2; // MINUMUM PARTS
          if (partitionResolver != null) {
            numParts++;
          }
          responseMsg.setNumberOfParts(numParts);
          // PART 1
          responseMsg.addObjPart(prRgion.getTotalNumberOfBuckets());
          
          // PART 2
          if (partitionResolver != null) {
            responseMsg.addObjPart(partitionResolver.getClass().toString()
                .substring(6));
          }
          
          // PART 3
          String leaderRegionPath=null;
          PartitionedRegion leaderRegion = null;
          String leaderRegionName = prRgion.getColocatedWith();
          if (leaderRegionName != null) {
            Cache cache = prRgion.getCache();
            while (leaderRegionName != null) {
              leaderRegion = (PartitionedRegion)cache
                  .getRegion(leaderRegionName);
              if (leaderRegion.getColocatedWith() == null) {
                leaderRegionPath=leaderRegion.getFullPath();
                break;
              } else {
               leaderRegionName = leaderRegion.getColocatedWith();
              }
            }
          }
          responseMsg.addObjPart(leaderRegionPath);
          responseMsg.send();
          msg.flush();
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
