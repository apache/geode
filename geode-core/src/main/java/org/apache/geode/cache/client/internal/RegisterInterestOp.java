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

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.InterestType;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.serialization.KnownVersion;

/**
 * Does a region registerInterest on a server
 *
 * @since GemFire 5.7
 */
public class RegisterInterestOp {
  /**
   * Does a region registerInterest on a server using connections from the given pool to communicate
   * with the server.
   *
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the registerInterest on
   * @param key describes what we are interested in
   * @param interestType the {@link InterestType} for this registration
   * @param policy the interest result policy for this registration
   * @param isDurable true if this registration is durable
   * @param regionDataPolicy the data policy ordinal of the region
   * @return list of keys
   */
  public static List execute(ExecutablePool pool, String region, Object key, int interestType,
      InterestResultPolicy policy, boolean isDurable, boolean receiveUpdatesAsInvalidates,
      byte regionDataPolicy) {
    AbstractOp op = new RegisterInterestOpImpl(region, key, interestType, policy, isDurable,
        receiveUpdatesAsInvalidates, regionDataPolicy);
    return (List) pool.executeOnQueuesAndReturnPrimaryResult(op);
  }

  /**
   * Does a region registerInterest on a server using connections from the given pool to communicate
   * with the given server location.
   *
   * @param sl the server to do the register interest on.
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the registerInterest on
   * @param key describes what we are interested in
   * @param interestType the {@link InterestType} for this registration
   * @param policy the interest result policy for this registration
   * @param isDurable true if this registration is durable
   * @param regionDataPolicy the data policy ordinal of the region
   * @return list of keys
   */
  public static List executeOn(ServerLocation sl, ExecutablePool pool, String region, Object key,
      int interestType, InterestResultPolicy policy, boolean isDurable,
      boolean receiveUpdatesAsInvalidates, byte regionDataPolicy) {
    AbstractOp op = new RegisterInterestOpImpl(region, key, interestType, policy, isDurable,
        receiveUpdatesAsInvalidates, regionDataPolicy);
    return (List) pool.executeOn(sl, op);
  }


  /**
   * Does a region registerInterest on a server using connections from the given pool to communicate
   * with the given server location.
   *
   * @param conn the connection to do the register interest on.
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the registerInterest on
   * @param key describes what we are interested in
   * @param interestType the {@link InterestType} for this registration
   * @param policy the interest result policy for this registration
   * @param isDurable true if this registration is durable
   * @param regionDataPolicy the data policy ordinal of the region
   * @return list of keys
   */
  public static List executeOn(Connection conn, ExecutablePool pool, String region, Object key,
      int interestType, InterestResultPolicy policy, boolean isDurable,
      boolean receiveUpdatesAsInvalidates, byte regionDataPolicy) {
    AbstractOp op = new RegisterInterestOpImpl(region, key, interestType, policy, isDurable,
        receiveUpdatesAsInvalidates, regionDataPolicy);
    return (List) pool.executeOn(conn, op);
  }


  private RegisterInterestOp() {
    // no instances allowed
  }

  protected static class RegisterInterestOpImpl extends AbstractOp {
    protected String region;

    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    public RegisterInterestOpImpl(String region, Object key, int interestType,
        InterestResultPolicy policy, boolean isDurable, boolean receiveUpdatesAsInvalidates,
        byte regionDataPolicy) {
      super(MessageType.REGISTER_INTEREST, 7);
      this.region = region;
      getMessage().addStringPart(region, true);
      getMessage().addIntPart(interestType);
      getMessage().addObjPart(policy);
      {
        byte durableByte = (byte) (isDurable ? 0x01 : 0x00);
        getMessage().addBytesPart(new byte[] {durableByte});
      }
      getMessage().addStringOrObjPart(key);
      byte notifyByte = (byte) (receiveUpdatesAsInvalidates ? 0x01 : 0x00);
      getMessage().addBytesPart(new byte[] {notifyByte});

      // The second byte '1' below tells server to serialize values in VersionObjectList.
      // Java clients always expect serializeValues to be true in VersionObjectList unlike Native
      // clients.
      // This was being sent as part of GetAllOp prior to fixing #43684.
      getMessage().addBytesPart(new byte[] {regionDataPolicy, (byte) 0x01});
    }

    /**
     * This constructor is used by our subclass CreateCQWithIROpImpl
     *
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    protected RegisterInterestOpImpl(String region, int msgType, int numParts) {
      super(msgType, numParts);
      this.region = region;
    }

    @Override
    protected Message createResponseMessage() {
      return new ChunkedMessage(1, KnownVersion.CURRENT);
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      throw new UnsupportedOperationException();
    }

    @Override
    protected Object processResponse(Message m, Connection con) throws Exception {
      ChunkedMessage chunkedMessage = (ChunkedMessage) m;
      chunkedMessage.readHeader();
      switch (chunkedMessage.getMessageType()) {
        case MessageType.RESPONSE_FROM_PRIMARY: {
          LocalRegion localRegion = null;

          try {
            localRegion = (LocalRegion) GemFireCacheImpl.getInstance().getRegion(this.region);
          } catch (Exception ignore) {
          }

          ArrayList list = new ArrayList();
          ArrayList listOfList = new ArrayList();
          listOfList.add(list);

          // Process the chunks
          List serverKeys = new ArrayList();
          VersionedObjectList serverEntries = null;
          do {
            // Read the chunk
            chunkedMessage.receiveChunk();

            // Deserialize the result
            Part part = chunkedMessage.getPart(0);

            Object partObj = part.getObject();
            if (partObj instanceof Throwable) {
              String s = "While performing a remote " + getOpName();
              throw new ServerOperationException(s, (Throwable) partObj);
              // Get the exception toString part.
              // This was added for c++ thin client and not used in java
            } else {
              if (partObj instanceof VersionedObjectList) {
                if (serverEntries == null) {
                  serverEntries = new VersionedObjectList(true);
                }
                ((VersionedObjectList) partObj).replaceNullIDs(con.getEndpoint().getMemberId());
                list.clear();
                list.add(partObj);

                if (localRegion != null) {
                  try {
                    localRegion.refreshEntriesFromServerKeys(con, listOfList,
                        InterestResultPolicy.KEYS_VALUES);
                  } catch (Exception ex) {
                  }
                }
              } else {
                // Add the result to the list of results
                serverKeys.add((List) partObj);
              }
            }

          } while (!chunkedMessage.isLastChunk());
          if (serverEntries != null) {
            list.clear();
            list.add(serverEntries); // serverEntries will always be empty.
            return listOfList;
          }
          return serverKeys;
        }
        case MessageType.RESPONSE_FROM_SECONDARY:
          // Read the chunk
          chunkedMessage.receiveChunk();
          return null;
        case MessageType.EXCEPTION:
          // Read the chunk
          chunkedMessage.receiveChunk();
          // Deserialize the result
          Part part = chunkedMessage.getPart(0);
          // Get the exception toString part.
          // This was added for c++ thin client and not used in java
          Object obj = part.getObject(); {
          String s = this + ": While performing a remote " + getOpName();
          throw new ServerOperationException(s, (Throwable) obj);
        }
        case MessageType.REGISTER_INTEREST_DATA_ERROR:
          // Read the chunk
          chunkedMessage.receiveChunk();

          // Deserialize the result
          String errorMessage = chunkedMessage.getPart(0).getString();
          String s = this + ": While performing a remote " + getOpName() + ": ";
          throw new ServerOperationException(s + errorMessage);
        default:
          throw new InternalGemFireError("Unknown message type " + chunkedMessage.getMessageType());
      }
    }

    protected String getOpName() {
      return "registerInterest";
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.REGISTER_INTEREST_DATA_ERROR;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startRegisterInterest();
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endRegisterInterestSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endRegisterInterest(start, hasTimedOut(), hasFailed());
    }
  }
}
