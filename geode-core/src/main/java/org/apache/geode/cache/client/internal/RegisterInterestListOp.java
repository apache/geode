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

import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.client.internal.RegisterInterestOp.RegisterInterestOpImpl;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.tier.MessageType;

/**
 * Does a region registerInterestList on a server
 *
 * @since GemFire 5.7
 */
public class RegisterInterestListOp {
  /**
   * Does a region registerInterestList on a server using connections from the given pool to
   * communicate with the server.
   *
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the registerInterestList on
   * @param keys list of keys we are interested in
   * @param policy the interest result policy for this registration
   * @param isDurable true if this registration is durable
   * @param regionDataPolicy the data policy ordinal of the region
   * @return list of keys
   */
  public static List execute(ExecutablePool pool, String region, List keys,
      InterestResultPolicy policy, boolean isDurable, boolean receiveUpdatesAsInvalidates,
      byte regionDataPolicy) {
    AbstractOp op = new RegisterInterestListOpImpl(region, keys, policy, isDurable,
        receiveUpdatesAsInvalidates, regionDataPolicy);
    return (List) pool.executeOnQueuesAndReturnPrimaryResult(op);
  }

  private RegisterInterestListOp() {
    // no instances allowed
  }

  /**
   * Does a region registerInterestList on a server using connections from the given pool to
   * communicate with the given server location.
   *
   * @param sl the server to do the register interest on.
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the registerInterest on
   * @param keys describes what we are interested in
   * @param policy the interest result policy for this registration
   * @param isDurable true if this registration is durable
   * @param regionDataPolicy the data policy ordinal of the region
   * @return list of keys
   */
  public static List executeOn(ServerLocation sl, ExecutablePool pool, String region, List keys,
      InterestResultPolicy policy, boolean isDurable, boolean receiveUpdatesAsInvalidates,
      byte regionDataPolicy) {
    AbstractOp op = new RegisterInterestListOpImpl(region, keys, policy, isDurable,
        receiveUpdatesAsInvalidates, regionDataPolicy);
    return (List) pool.executeOn(sl, op);
  }


  /**
   * Does a region registerInterestList on a server using connections from the given pool to
   * communicate with the given server location.
   *
   * @param conn the connection to do the register interest on.
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the registerInterest on
   * @param keys describes what we are interested in
   * @param policy the interest result policy for this registration
   * @param isDurable true if this registration is durable
   * @param regionDataPolicy the data policy ordinal of the region
   * @return list of keys
   */
  public static List executeOn(ClientCacheConnection conn, ExecutablePool pool, String region,
      List keys,
      InterestResultPolicy policy, boolean isDurable, boolean receiveUpdatesAsInvalidates,
      byte regionDataPolicy) {
    AbstractOp op = new RegisterInterestListOpImpl(region, keys, policy, isDurable,
        receiveUpdatesAsInvalidates, regionDataPolicy);
    return (List) pool.executeOn(conn, op);
  }

  private static class RegisterInterestListOpImpl extends RegisterInterestOpImpl {
    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    public RegisterInterestListOpImpl(String region, List keys, InterestResultPolicy policy,
        boolean isDurable, boolean receiveUpdatesAsInvalidates, byte regionDataPolicy) {
      super(region, MessageType.REGISTER_INTEREST_LIST, 6);
      getMessage().addStringPart(region, true);
      getMessage().addObjPart(policy);
      {
        byte durableByte = (byte) (isDurable ? 0x01 : 0x00);
        getMessage().addBytesPart(new byte[] {durableByte});
      }
      // Set chunk size of HDOS for keys
      getMessage().setChunkSize(keys.size() * 16);
      getMessage().addObjPart(keys);

      byte notifyByte = (byte) (receiveUpdatesAsInvalidates ? 0x01 : 0x00);
      getMessage().addBytesPart(new byte[] {notifyByte});

      // The second byte '1' below tells server to serialize values in VersionObjectList.
      // Java clients always expect serializeValues to be true in VersionObjectList unlike Native
      // clients.
      // This was being sent as part of GetAllOp prior to fixing #43684.
      getMessage().addBytesPart(new byte[] {regionDataPolicy, (byte) 0x01});
    }

    @Override
    protected String getOpName() {
      return "registerInterestList";
    }
    // note we reuse the same stats used by RegisterInterestOpImpl
  }
}
