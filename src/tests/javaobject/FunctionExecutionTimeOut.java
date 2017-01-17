/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
