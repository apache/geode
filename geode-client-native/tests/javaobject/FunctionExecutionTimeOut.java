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
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;

import com.gemstone.gemfire.internal.cache.BridgeServerImpl;
import com.gemstone.gemfire.internal.cache.tier.ClientHandShake;
import com.gemstone.gemfire.internal.cache.tier.sockets.AcceptorImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;

public class FunctionExecutionTimeOut extends FunctionAdapter implements
    Declarable {

  public void execute(FunctionContext context) {
    DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    LogWriter logger = ds.getLogWriter();
    logger.fine("Executing FunctionExecutionTimeOut ");
    int expected = (Integer)context.getArguments();
	boolean timeoutFound = false;
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    AcceptorImpl acceptor = ((BridgeServerImpl) cache.getCacheServers().get(0)).getAcceptor();
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
