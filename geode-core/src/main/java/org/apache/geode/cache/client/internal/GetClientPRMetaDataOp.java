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
package org.apache.geode.cache.client.internal;

import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.internal.cache.BucketServerLocation66;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.logging.LogService;

/**
 * Retrieves {@link ClientPartitionAdvisor} for the specified PartitionedRegion from one of the
 * servers
 *
 *
 * @since GemFire 6.5
 */
public class GetClientPRMetaDataOp {

  private static final Logger logger = LogService.getLogger();

  private GetClientPRMetaDataOp() {
    // no instances allowed
  }

  public static void execute(ExecutablePool pool, String regionFullPath,
      ClientMetadataService cms) {
    AbstractOp op = new GetClientPRMetaDataOpImpl(regionFullPath, cms);
    if (logger.isDebugEnabled()) {
      logger.debug(
          "GetClientPRMetaDataOp#execute : Sending GetClientPRMetaDataOp Message: {} to server using pool: {}",
          op.getMessage(), pool);
    }
    pool.execute(op);
  }

  static class GetClientPRMetaDataOpImpl extends AbstractOp {

    String regionFullPath = null;

    ClientMetadataService cms = null;

    public GetClientPRMetaDataOpImpl(String regionFullPath, ClientMetadataService cms) {
      super(MessageType.GET_CLIENT_PR_METADATA, 1);
      this.regionFullPath = regionFullPath;
      this.cms = cms;
      getMessage().addStringPart(regionFullPath);
    }

    @Override
    protected boolean needsUserId() {
      return false;
    }

    @Override
    protected void sendMessage(Connection cnx) throws Exception {
      getMessage().clearMessageHasSecurePartFlag();
      getMessage().send(false);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Object processResponse(Message msg) throws Exception {
      switch (msg.getMessageType()) {
        case MessageType.GET_CLIENT_PR_METADATA_ERROR:
          String errorMsg = msg.getPart(0).getString();
          if (logger.isDebugEnabled()) {
            logger.debug(errorMsg);
          }
          throw new ServerOperationException(errorMsg);
        case MessageType.RESPONSE_CLIENT_PR_METADATA:
          final boolean isDebugEnabled = logger.isDebugEnabled();
          if (isDebugEnabled) {
            logger.debug("GetClientPRMetaDataOpImpl#processResponse: received message of type : {}"
                + MessageType.getString(msg.getMessageType()));
          }
          int numParts = msg.getNumberOfParts();
          ClientPartitionAdvisor advisor = cms.getClientPartitionAdvisor(regionFullPath);
          for (int i = 0; i < numParts; i++) {
            Object result = msg.getPart(i).getObject();
            List<BucketServerLocation66> locations = (List<BucketServerLocation66>) result;
            if (!locations.isEmpty()) {
              int bucketId = locations.get(0).getBucketId();
              if (isDebugEnabled) {
                logger.debug(
                    "GetClientPRMetaDataOpImpl#processResponse: for bucketId : {} locations are {}",
                    bucketId, locations);
              }
              advisor.updateBucketServerLocations(bucketId, locations, cms);

              Set<ClientPartitionAdvisor> cpas =
                  cms.getColocatedClientPartitionAdvisor(regionFullPath);
              if (cpas != null && !cpas.isEmpty()) {
                for (ClientPartitionAdvisor colCPA : cpas) {
                  colCPA.updateBucketServerLocations(bucketId, locations, cms);
                }
              }
            }
          }
          if (isDebugEnabled) {
            logger.debug(
                "GetClientPRMetaDataOpImpl#processResponse: received ClientPRMetadata from server successfully.");
          }
          cms.setMetadataStable(true);
          return null;
        case MessageType.EXCEPTION:
          if (logger.isDebugEnabled()) {
            logger.debug(
                "GetClientPRMetaDataOpImpl#processResponse: received message of type EXCEPTION");
          }
          Part part = msg.getPart(0);
          Object obj = part.getObject();
          String s = "While performing  GetClientPRMetaDataOp " + ((Throwable) obj).getMessage();
          throw new ServerOperationException(s, (Throwable) obj);
        default:
          throw new InternalGemFireError(String.format("Unknown message type %s",
              Integer.valueOf(msg.getMessageType())));
      }
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startGetClientPRMetadata();
    }

    protected String getOpName() {
      return "GetClientPRMetaDataOp";
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endGetClientPRMetadataSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endGetClientPRMetadata(start, hasTimedOut(), hasFailed());
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return false;
    }
  }

}
