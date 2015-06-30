package com.gemstone.gemfire.cache.hdfs.internal;

import java.io.DataOutput;
import java.io.IOException;

public interface QueuedPersistentEvent {
  
  public byte[] getRawKey();
  
  public void toHoplogEventBytes(DataOutput out) throws IOException;
}
