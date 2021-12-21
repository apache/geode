/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

package org.apache.geode.cache.lucene.internal.filesystem;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * The key for a single chunk on a file stored within a region.
 */
public class ChunkKey implements DataSerializableFixedID {
  UUID fileId;
  int chunkId;

  /**
   * Constructor used for serialization only.
   */
  public ChunkKey() {}

  ChunkKey(UUID fileName, int chunkId) {
    fileId = fileName;
    this.chunkId = chunkId;
  }

  /**
   * @return the fileName
   */
  public UUID getFileId() {
    return fileId;
  }

  /**
   * @return the chunkId
   */
  public int getChunkId() {
    return chunkId;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + fileId.hashCode();
    result = prime * result + chunkId;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof ChunkKey)) {
      return false;
    }
    ChunkKey other = (ChunkKey) obj;
    if (chunkId != other.chunkId) {
      return false;
    }
    if (fileId == null) {
      return other.fileId == null;
    } else
      return fileId.equals(other.fileId);
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getDSFID() {
    return LUCENE_CHUNK_KEY;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    out.writeInt(chunkId);
    out.writeLong(fileId.getMostSignificantBits());
    out.writeLong(fileId.getLeastSignificantBits());
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    chunkId = in.readInt();
    long high = in.readLong();
    long low = in.readLong();
    fileId = new UUID(high, low);
  }


}
