package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.internal.cache.DiskEntry.Helper.ValueWrapper;

public interface OplogSet {
  
  
  public void create(LocalRegion region, DiskEntry entry, ValueWrapper value,
      boolean async);
  
  public void modify(LocalRegion region, DiskEntry entry, ValueWrapper value,
      boolean async);

  public CompactableOplog getChild(long oplogId);

  public void remove(LocalRegion region, DiskEntry entry, boolean async,
      boolean isClear);
}
