/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.HashSet;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.CacheServerImpl;

import org.apache.geode.internal.cache.tier.ClientHandShake;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.LogWriter;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;

public class FunctionExecutionTimeOut extends FunctionAdapter implements
    Declarable {

  public void execute(FunctionContext context) {
    DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    LogWriter logger = ds.getLogWriter();
    logger.fine("Executing FunctionExecutionTimeOut ");
    int expected = (Integer)context.getArguments();
	boolean timeoutFound = false;
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    AcceptorImpl acceptor = ((CacheServerImpl) cache.getCacheServers().get(0)).getAcceptor();
    ServerConnection[] scs = acceptor.getAllServerConnectionList();
    for (int i = 0; i < scs.length; ++i) {
      ClientHandShake hs = scs[i].getHandshake();
	  if (hs != null) {
	    logger.fine("hs.getClientReadTimeout() =  " + hs.getClientReadTimeout());
	  }
      if (hs != null && expected == hs.getClientReadTimeout()) {
        // success
		timeoutFound = true;
		break;
      }
    }
	context.getResultSender().lastResult(timeoutFound);
  }
      	  
  public boolean isHA() {
    return false;
  }
	
  public String getId() {    
    return "FunctionExecutionTimeOut";
  }

  public void init(Properties props) {
  }

}
