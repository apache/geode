package com.gemstone.gemfire.cache.query.internal.cq;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.ServiceLoader;

import com.gemstone.gemfire.cache.query.internal.cq.spi.CqServiceFactory;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;

public class CqServiceProvider {
  
  private static final CqServiceFactory factory;
  // System property to maintain the CQ event references for optimizing the updates.
  // This will allows to run the CQ query only once during update events.   
  public static boolean MAINTAIN_KEYS = 
    Boolean.valueOf(System.getProperty("gemfire.cq.MAINTAIN_KEYS", "true")).booleanValue();
  /**
   * A debug flag used for testing vMotion during CQ registration
   */
  public static boolean VMOTION_DURING_CQ_REGISTRATION_FLAG = false;
  
  
  static {
    ServiceLoader<CqServiceFactory> loader = ServiceLoader.load(CqServiceFactory.class);
    Iterator<CqServiceFactory> itr = loader.iterator();
    if(!itr.hasNext()) {
      factory = null;
    } else {
      factory = itr.next();
      factory.initialize();
    }
  }
  
  public static CqService create(GemFireCacheImpl cache) {
    
    if(factory == null) {
      return new MissingCqService();
    }
    
    return factory.create(cache);
  }
  
  public static ServerCQ readCq(DataInput in) throws ClassNotFoundException, IOException {
    if(factory == null) {
      throw new IllegalStateException("CqService is not available.");
    } else {
      return factory.readCqQuery(in);
    }
    
  }

  private CqServiceProvider() {
    
  }
}
