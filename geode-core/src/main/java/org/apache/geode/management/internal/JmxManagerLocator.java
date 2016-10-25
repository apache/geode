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
package org.apache.geode.management.internal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.SharedConfiguration;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.tcpserver.TcpHandler;
import org.apache.geode.distributed.internal.tcpserver.TcpServer;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.AlreadyRunningException;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.JmxManagerAdvisor.JmxManagerProfile;

public class JmxManagerLocator implements TcpHandler {
  private static final Logger logger = LogService.getLogger();
  
  private GemFireCacheImpl cache;

  public JmxManagerLocator(GemFireCacheImpl gfc) {
    this.cache = gfc;
  }

  @Override
  public Object processRequest(Object request) throws IOException {
    assert request instanceof JmxManagerLocatorRequest;
    return findJmxManager((JmxManagerLocatorRequest)request);
  }

  @Override
  public void endRequest(Object request, long startTime) {
    // nothing needed
  }

  @Override
  public void endResponse(Object request, long startTime) {
    // nothing needed
  }

  @Override
  public void shutDown() {
    // nothing needed
  }

  public void restarting(DistributedSystem ds, GemFireCache cache, SharedConfiguration sharedConfig) {
    this.cache = (GemFireCacheImpl)cache;
  }

  @Override
  public void init(TcpServer tcpServer) {
    // nothing needed
  }

  private JmxManagerLocatorResponse findJmxManager(JmxManagerLocatorRequest request) {
    JmxManagerLocatorResponse result = null;
    if (logger.isDebugEnabled()) {
      logger.debug("Locator requested to find or start jmx manager");
    }
    List<JmxManagerProfile> alreadyManaging = this.cache.getJmxManagerAdvisor().adviseAlreadyManaging();
    if (alreadyManaging.isEmpty()) {
      List<JmxManagerProfile> willingToManage = this.cache.getJmxManagerAdvisor().adviseWillingToManage();
      if (!willingToManage.isEmpty()) {
        synchronized (this) {
          alreadyManaging = this.cache.getJmxManagerAdvisor().adviseAlreadyManaging();
          if (alreadyManaging.isEmpty()) {
            willingToManage = this.cache.getJmxManagerAdvisor().adviseWillingToManage();
            if (!willingToManage.isEmpty()) {
              JmxManagerProfile p = willingToManage.get(0);
              if (p.getDistributedMember().equals(this.cache.getMyId())) {
                if (logger.isDebugEnabled()) {
                  logger.debug("Locator starting jmx manager in its JVM");
                }
                try {
                  ManagementService.getManagementService(this.cache).startManager();
                } catch (CancelException ex) {
                  // ignore
                } catch (VirtualMachineError err) {
                  SystemFailure.initiateFailure(err);
                  // If this ever returns, rethrow the error.  We're poisoned
                  // now, so don't let this thread continue.
                  throw err;
                } catch (Throwable t) {
                  SystemFailure.checkFailure();
                  return new JmxManagerLocatorResponse(null, 0, false, t);
                }
              } else {
                p = startJmxManager(willingToManage);
                if (p != null) {
                  if (logger.isDebugEnabled()) {
                    logger.debug("Locator started jmx manager in {}", p.getDistributedMember());
                  }
                }
                // bug 46041 is caused by a race in which the function reply comes
                // before we have received the profile update. So pause for a bit
                // if our advisor still does not know about a manager and the member
                // we asked to start one is still in the ds.
                alreadyManaging = this.cache.getJmxManagerAdvisor().adviseAlreadyManaging();
                int sleepCount = 0;
                while (sleepCount < 20 && alreadyManaging.isEmpty() && this.cache.getDistributionManager().getDistributionManagerIds().contains(p.getDistributedMember())) {
                  sleepCount++;
                  try {
                    Thread.sleep(100);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                  alreadyManaging = this.cache.getJmxManagerAdvisor().adviseAlreadyManaging();
                }
              }
              if (alreadyManaging.isEmpty()) {
                alreadyManaging = this.cache.getJmxManagerAdvisor().adviseAlreadyManaging();
              }
            }
          }
        } // sync
      }
    }
    if (!alreadyManaging.isEmpty()) {
      JmxManagerProfile p = alreadyManaging.get(0);
      result = new JmxManagerLocatorResponse(p.getHost(), p.getPort(), p.getSsl(), null);
      if (logger.isDebugEnabled()) {
        logger.debug("Found jmx manager: " + p);
      }
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug("Did not find a jmx manager");
      }
      result = new JmxManagerLocatorResponse();
    }
    return result;
  }

  private JmxManagerProfile startJmxManager(List<JmxManagerProfile> willingToManage) {
    for (JmxManagerProfile p: willingToManage) {
      if (sendStartJmxManager(p.getDistributedMember())) {
        return p;
      }
    }
    return null;
  }

  private boolean sendStartJmxManager(InternalDistributedMember distributedMember) {
    try {
    ArrayList<Object> resultContainer = (ArrayList<Object>)FunctionService.onMember(distributedMember).execute(new StartJmxManagerFunction()).getResult();
    Object result = resultContainer.get(0);
    if (result instanceof Boolean) {
      return ((Boolean)result).booleanValue();
    } else {
      logger.info("Could not start jmx manager on {} because {}", distributedMember, result);
      return false;
    }
    } catch (RuntimeException ex) {
      if (!this.cache.getDistributionManager().getDistributionManagerIdsIncludingAdmin().contains(distributedMember)) {
        // if the member went away then just return false
        logger.info("Could not start jmx manager on {} because of {}", distributedMember, ex.getMessage(), ex);
        return false;
      } else {
        throw ex;
      }
    }
  }
  
  public static class StartJmxManagerFunction implements Function, InternalEntity {
    private static final long serialVersionUID = -2860286061903069789L;
    public static final String ID = StartJmxManagerFunction.class.getName();

    @Override
    public void execute(FunctionContext context) {
      
      try {
        Cache cache = CacheFactory.getAnyInstance();
        if (cache != null) {
          ManagementService ms = ManagementService.getExistingManagementService(cache);
          if (ms != null) {
            if (!ms.isManager()) { // see bug 45922
              ms.startManager();
            }
            context.getResultSender().lastResult(Boolean.TRUE);
          }
        }
        context.getResultSender().lastResult(Boolean.FALSE);
      } catch (AlreadyRunningException ok) {
        context.getResultSender().lastResult(Boolean.TRUE);
      }catch (Exception e) {
        context.getResultSender().lastResult(
            "Exception in StartJmxManager =" + e.getMessage());
      }
    }

    @Override
    public String getId() {
      return StartJmxManagerFunction.ID;
    }

    @Override
    public boolean hasResult() {
      return true;
    }

    @Override
    public boolean optimizeForWrite() {
      // no need of optimization since read-only.
      return false;
    }

    @Override
    public boolean isHA() {
      return false;
    }

  }
}
