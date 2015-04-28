package com.gemstone.gemfire.cache.query.internal.cq.spi;

import java.io.DataInput;
import java.io.IOException;

import com.gemstone.gemfire.cache.query.internal.cq.CqService;
import com.gemstone.gemfire.cache.query.internal.cq.ServerCQ;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;

public interface CqServiceFactory {
  
  public void initialize();
  
  /**
   * Create a new CqService for the given cache
   */
  public CqService create(GemFireCacheImpl cache);

  public ServerCQ readCqQuery(DataInput in) throws ClassNotFoundException, IOException;
}
