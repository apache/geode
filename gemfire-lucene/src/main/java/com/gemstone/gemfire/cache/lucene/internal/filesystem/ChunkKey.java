package com.gemstone.gemfire.cache.lucene.internal.filesystem;

import java.io.Serializable;
import java.util.UUID;

/**
 * The key for a single chunk on a file stored within a region.
 */
public class ChunkKey implements Serializable {

  private static final long serialVersionUID = 1L;

  UUID fileId;
  int chunkId;

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

  
}
