package com.gemstone.gemfire.cache.lucene.internal;

import java.io.Serializable;

public class ChunkKey implements Serializable {

  private static final long serialVersionUID = 1L;

  String fileName;
  int chunkId;

  ChunkKey(String fileName, int chunkId) {
    this.fileName = fileName;
    this.chunkId = chunkId;
  }

  /**
   * @return the fileName
   */
  public String getFileName() {
    return fileName;
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
    result = prime * result + fileName.hashCode();
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
    if (fileName == null) {
      if (other.fileName != null) {
        return false;
      }
    } else if (!fileName.equals(other.fileName)) {
      return false;
    }
    return true;
  }

  
}
