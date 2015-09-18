package com.gemstone.gemfire.cache.lucene.internal.filesystem;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;

/**
 * The key for a single chunk on a file stored within a region.
 */
public class ChunkKey implements DataSerializableFixedID {
  UUID fileId;
  int chunkId;
  
  /**
   * Constructor used for serialization only.
   */
  public ChunkKey() {
  }

  ChunkKey(UUID fileName, int chunkId) {
    this.fileId = fileName;
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
      if (other.fileId != null) {
        return false;
      }
    } else if (!fileId.equals(other.fileId)) {
      return false;
    }
    return true;
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getDSFID() {
    return LUCENE_CHUNK_KEY;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeInt(chunkId);
    out.writeLong(fileId.getMostSignificantBits());
    out.writeLong(fileId.getLeastSignificantBits());
  }

  @Override
  public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
    chunkId = in.readInt();
    long high = in.readLong();
    long low = in.readLong();
    fileId = new UUID(high, low);
  }

  
}
