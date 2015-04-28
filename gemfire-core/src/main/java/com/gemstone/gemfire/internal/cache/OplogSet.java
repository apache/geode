package com.gemstone.gemfire.internal.cache;

import java.util.concurrent.atomic.AtomicLong;

public interface OplogSet {
  
  
  public void create(LocalRegion region, DiskEntry entry, byte[] value,
      boolean isSerializedObject, boolean async);
  
  public void modify(LocalRegion region, DiskEntry entry, byte[] value,
      boolean isSerializedObject, boolean async);

  public CompactableOplog getChild(long oplogId);

  public void remove(LocalRegion region, DiskEntry entry, boolean async,
      boolean isClear);
}
