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
package org.apache.geode.cache.query.internal.cq;

import org.apache.geode.cache.query.internal.cq.spi.CqServiceFactory;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.GemFireCacheImpl;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.ServiceLoader;

public class CqServiceProvider {
  
  private static final CqServiceFactory factory;
  // System property to maintain the CQ event references for optimizing the updates.
  // This will allows to run the CQ query only once during update events.   
  public static boolean MAINTAIN_KEYS =
      Boolean.valueOf(System.getProperty(DistributionConfig.GEMFIRE_PREFIX + "cq.MAINTAIN_KEYS", "true")).booleanValue();
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
