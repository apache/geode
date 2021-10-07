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
 *
 */

package org.apache.geode.redis.internal.data;


import static org.apache.geode.redis.internal.RedisConstants.ERROR_RESTORE_INVALID_PAYLOAD;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bRADISH_DUMP_HEADER;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.geode.DataSerializer;
import org.apache.geode.Delta;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.VersionedDataInputStream;
import org.apache.geode.internal.size.Sizeable;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.RedisException;
import org.apache.geode.redis.internal.RegionProvider;

public interface RedisData extends Delta, DataSerializableFixedID, Sizeable {

  /**
   * Returns true if this instance does not exist.
   */
  default boolean isNull() {
    return false;
  }

  default boolean exists() {
    return !isNull();
  }

  RedisDataType getType();

  void setExpirationTimestamp(Region<RedisKey, RedisData> region, RedisKey key, long value);

  long getExpirationTimestamp();

  int persist(Region<RedisKey, RedisData> region, RedisKey key);

  boolean hasExpired();

  boolean hasExpired(long now);

  long pttl(Region<RedisKey, RedisData> region, RedisKey key);

  int pexpireat(RegionProvider regionProvider, RedisKey key, long timestamp);

  void doExpiration(RegionProvider regionProvider, RedisKey key);

  String type();

  boolean rename(Region<RedisKey, RedisData> region, RedisKey oldKey, RedisKey newKey,
      boolean ifTargetNotExists);

  default boolean getForceRecalculateSize() {
    return true;
  }

  byte[] dump() throws IOException;

  RedisData restore(byte[] data, boolean replaceExisting) throws Exception;

  default RedisData restore(byte[] data) throws Exception {
    Object obj;
    byte[] header = new byte[bRADISH_DUMP_HEADER.length];

    try {
      ByteArrayInputStream bais = new ByteArrayInputStream(data);
      bais.read(header);

      // Don't handle Redis dump formats yet
      if (!Arrays.equals(header, bRADISH_DUMP_HEADER)) {
        throw new RedisException(ERROR_RESTORE_INVALID_PAYLOAD);
      }

      DataInputStream inputStream = new DataInputStream(bais);
      short ordinal = inputStream.readShort();
      KnownVersion knownVersion = KnownVersion.getKnownVersion(ordinal);
      if (knownVersion == null) {
        LogService.getLogger()
            .info("Error restoring object - unknown version ordinal: {}", ordinal);
        throw new RedisException(ERROR_RESTORE_INVALID_PAYLOAD);
      }

      obj = DataSerializer.readObject(new VersionedDataInputStream(bais, knownVersion));
    } catch (Exception ex) {
      LogService.getLogger().warn("Exception restoring data", ex);
      throw new RedisException(ERROR_RESTORE_INVALID_PAYLOAD, ex);
    }

    return (RedisData) obj;
  }

}
