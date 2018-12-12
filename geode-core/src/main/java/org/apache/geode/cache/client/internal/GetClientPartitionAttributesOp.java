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

import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.FixedPartitionAttributes;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.logging.LogService;

/**
 *
 * Retrieves {@link ClientPartitionAdvisor} related information for the specified PartitionedRegion
 * from one of the servers
 *
 *
 * @since GemFire 6.5
 *
 */
public class GetClientPartitionAttributesOp {

  private static final Logger logger = LogService.getLogger();

  private GetClientPartitionAttributesOp() {
    // no instances allowed
  }

  @SuppressWarnings("unchecked")
  public static ClientPartitionAdvisor execute(ExecutablePool pool, String regionFullPath) {
    AbstractOp op = new GetClientPartitionAttributesOpImpl(regionFullPath);
    if (logger.isDebugEnabled()) {
      logger.debug(
          "GetClientPartitionAttributesOp#execute : Sending GetClientPartitionAttributesOp Message: {} for region: {} to server using pool: {}",
          op.getMessage(), regionFullPath, pool);
    }

    ClientPartitionAdvisor advisor = (ClientPartitionAdvisor) pool.execute(op);

    if (advisor != null) {
      advisor.setServerGroup(((PoolImpl) pool).getServerGroup());
    }

    return advisor;
  }

  static class GetClientPartitionAttributesOpImpl extends AbstractOp {

    String regionFullPath = null;

    public GetClientPartitionAttributesOpImpl(String regionFullPath) {
      super(MessageType.GET_CLIENT_PARTITION_ATTRIBUTES, 1);
      this.regionFullPath = regionFullPath;
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
        case MessageType.GET_CLIENT_PARTITION_ATTRIBUTES_ERROR:
          String errorMsg = msg.getPart(0).getString();
          if (logger.isDebugEnabled()) {
            logger.debug(errorMsg);
          }
          throw new ServerOperationException(errorMsg);
        case MessageType.RESPONSE_CLIENT_PARTITION_ATTRIBUTES:
          final boolean isDebugEnabled = logger.isDebugEnabled();
          if (isDebugEnabled) {
            logger.debug(
                "GetClientPartitionAttributesOpImpl#processResponse: received message of type : {}",
                MessageType.getString(msg.getMessageType()));
          }
          int bucketCount;
          String colocatedWith;
          String partitionResolverName = null;
          Set<FixedPartitionAttributes> fpaSet = null;
          bucketCount = (Integer) msg.getPart(0).getObject();
          colocatedWith = (String) msg.getPart(1).getObject();
          if (msg.getNumberOfParts() == 4) {
            partitionResolverName = (String) msg.getPart(2).getObject();
            fpaSet = (Set<FixedPartitionAttributes>) msg.getPart(3).getObject();
          } else if (msg.getNumberOfParts() == 3) {
            Object obj = msg.getPart(2).getObject();
            if (obj instanceof String) {
              partitionResolverName = (String) obj;
            } else {
              fpaSet = (Set<FixedPartitionAttributes>) obj;
            }
          } else if (bucketCount == -1) {
            return null;
          }
          if (isDebugEnabled) {
            logger.debug(
                "GetClientPartitionAttributesOpImpl#processResponse: received all the results from server successfully.");
          }
          ClientPartitionAdvisor advisor =
              new ClientPartitionAdvisor(bucketCount, colocatedWith, partitionResolverName, fpaSet);
          return advisor;

        case MessageType.EXCEPTION:
          if (logger.isDebugEnabled()) {
            logger.debug(
                "GetClientPartitionAttributesOpImpl#processResponse: received message of type EXCEPTION");
          }
          Part part = msg.getPart(0);
          Object obj = part.getObject();
          String s =
              "While performing  GetClientPartitionAttributesOp " + ((Throwable) obj).getMessage();
          throw new ServerOperationException(s, (Throwable) obj);
        default:
          throw new InternalGemFireError(String.format("Unknown message type %s",
              Integer.valueOf(msg.getMessageType())));
      }
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startGetClientPartitionAttributes();
    }

    protected String getOpName() {
      return "GetClientPartitionAttributesOp";
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endGetClientPartitionAttributesSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endGetClientPartitionAttributes(start, hasTimedOut(), hasFailed());
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return false;
    }

  }

}
