/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.cache.client.internal;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.Logger;

import org.apache.geode.GemFireException;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.client.internal.GetAllOp.GetAllOpImpl;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PutAllPartialResultException;
import org.apache.geode.internal.cache.execute.BucketMovedException;
import org.apache.geode.internal.cache.execute.InternalFunctionInvocationTargetException;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;

public class SingleHopClientExecutor {

  private static final Logger logger = LogService.getLogger();

  static final ExecutorService execService = Executors.newCachedThreadPool(new ThreadFactory() {
    AtomicInteger threadNum = new AtomicInteger();

    public Thread newThread(final Runnable r) {
      Thread result = new Thread(r, "Function Execution Thread-" + threadNum.incrementAndGet());
      result.setDaemon(true);
      return result;
    }
  });

  static void submitAll(List callableTasks) {
    if (callableTasks != null && !callableTasks.isEmpty()) {
      List futures = null;
      try {
        futures = execService.invokeAll(callableTasks);
      } catch (RejectedExecutionException rejectedExecutionEx) {
        throw rejectedExecutionEx;
      } catch (InterruptedException e) {
        throw new InternalGemFireException(e.getMessage());
      }
      if (futures != null) {
        Iterator itr = futures.iterator();
        while (itr.hasNext() && !execService.isShutdown() && !execService.isTerminated()) {
          Future fut = (Future) itr.next();
          try {
            fut.get();
          } catch (InterruptedException e) {
            throw new InternalGemFireException(e.getMessage());
          } catch (ExecutionException ee) {
            if (ee.getCause() instanceof FunctionException) {
              throw (FunctionException) ee.getCause();
            } else if (ee.getCause() instanceof ServerOperationException) {
              throw (ServerOperationException) ee.getCause();
            } else if (ee.getCause() instanceof ServerConnectivityException) {
              throw (ServerConnectivityException) ee.getCause();
            } else {
              throw executionThrowable(ee.getCause());
            }
          }
        }
      }
    }
  }

  static boolean submitAllHA(List callableTasks, LocalRegion region, boolean isHA,
      ResultCollector rc, Set<String> failedNodes) {

    ClientMetadataService cms = region.getCache().getClientMetadataService();
    boolean reexecute = false;

    if (callableTasks != null && !callableTasks.isEmpty()) {
      List futures = null;
      try {
        futures = execService.invokeAll(callableTasks);
      } catch (RejectedExecutionException rejectedExecutionEx) {
        throw rejectedExecutionEx;
      } catch (InterruptedException e) {
        throw new InternalGemFireException(e.getMessage());
      }
      if (futures != null) {
        GemFireException functionExecutionException = null;
        Iterator futureItr = futures.iterator();
        Iterator taskItr = callableTasks.iterator();
        final boolean isDebugEnabled = logger.isDebugEnabled();
        while (futureItr.hasNext() && !execService.isShutdown() && !execService.isTerminated()) {
          Future fut = (Future) futureItr.next();
          SingleHopOperationCallable task = (SingleHopOperationCallable) taskItr.next();
          ServerLocation server = task.getServer();
          try {
            fut.get();
            if (isDebugEnabled) {
              logger.debug("ExecuteRegionFunctionSingleHopOp#got result from {}", server);
            }
          } catch (InterruptedException e) {
            throw new InternalGemFireException(e.getMessage());
          } catch (ExecutionException ee) {
            if (ee.getCause() instanceof InternalFunctionInvocationTargetException) {
              if (isDebugEnabled) {
                logger.debug(
                    "ExecuteRegionFunctionSingleHopOp#ExecutionException.InternalFunctionInvocationTargetException : Caused by :{}",
                    ee.getCause());
              }
              try {
                cms = region.getCache().getClientMetadataService();
              } catch (CacheClosedException e) {
                return false;
              }
              cms.removeBucketServerLocation(server);
              cms.scheduleGetPRMetaData(region, false);
              reexecute = true;
              failedNodes.addAll(
                  ((InternalFunctionInvocationTargetException) ee.getCause()).getFailedNodeSet());
              // Clear the results only if isHA so that partial results can be returned.
              if (isHA) {
                rc.clearResults();
              } else {
                if (ee.getCause().getCause() != null) {
                  functionExecutionException =
                      new FunctionInvocationTargetException(ee.getCause().getCause());
                } else {
                  functionExecutionException =
                      new FunctionInvocationTargetException(new BucketMovedException(
                          LocalizedStrings.FunctionService_BUCKET_MIGRATED_TO_ANOTHER_NODE
                              .toLocalizedString()));
                }
              }
            } else if (ee.getCause() instanceof FunctionException) {
              if (isDebugEnabled) {
                logger.debug(
                    "ExecuteRegionFunctionSingleHopOp#ExecutionException.FunctionException : Caused by :{}",
                    ee.getCause());
              }
              FunctionException fe = (FunctionException) ee.getCause();
              if (isHA) {
                throw fe;
              } else {
                functionExecutionException = fe;
              }
            } else if (ee.getCause() instanceof ServerOperationException) {
              if (isDebugEnabled) {
                logger.debug(
                    "ExecuteRegionFunctionSingleHopOp#ExecutionException.ServerOperationException : Caused by :{}",
                    ee.getCause());
              }
              ServerOperationException soe = (ServerOperationException) ee.getCause();
              if (isHA) {
                throw soe;
              } else {
                functionExecutionException = soe;
              }
            } else if (ee.getCause() instanceof ServerConnectivityException) {
              if (isDebugEnabled) {
                logger.debug(
                    "ExecuteRegionFunctionSingleHopOp#ExecutionException.ServerConnectivityException : Caused by :{} The failed server is: {}",
                    ee.getCause(), server);
              }
              try {
                cms = region.getCache().getClientMetadataService();
              } catch (CacheClosedException e) {
                return false;
              }
              cms.removeBucketServerLocation(server);
              cms.scheduleGetPRMetaData(region, false);
              // Clear the results only if isHA so that partial results can be returned.
              if (isHA) {
                reexecute = true;
                rc.clearResults();
              } else {
                functionExecutionException = (ServerConnectivityException) ee.getCause();
              }
            } else {
              throw executionThrowable(ee.getCause());
            }
          }
        }
        if (functionExecutionException != null) {
          throw functionExecutionException;
        }
      }
    }
    return reexecute;
  }

  /**
   * execute bulk op (putAll or removeAll) on multiple PR servers, returning a map of the results.
   * Results are either a VersionedObjectList or a BulkOpPartialResultsException
   * 
   * @param callableTasks
   * @param cms
   * @param region
   * @param failedServers
   * @return the per-server results
   */
  static Map<ServerLocation, Object> submitBulkOp(List callableTasks, ClientMetadataService cms,
      LocalRegion region, Map<ServerLocation, RuntimeException> failedServers) {
    if (callableTasks != null && !callableTasks.isEmpty()) {
      Map<ServerLocation, Object> resultMap = new HashMap<ServerLocation, Object>();
      boolean anyPartialResults = false;
      List futures = null;
      try {
        futures = execService.invokeAll(callableTasks);
      } catch (RejectedExecutionException rejectedExecutionEx) {
        throw rejectedExecutionEx;
      } catch (InterruptedException e) {
        throw new InternalGemFireException(e.getMessage());
      }
      if (futures != null) {
        Iterator futureItr = futures.iterator();
        Iterator taskItr = callableTasks.iterator();
        RuntimeException rte = null;
        final boolean isDebugEnabled = logger.isDebugEnabled();
        while (futureItr.hasNext() && !execService.isShutdown() && !execService.isTerminated()) {
          Future fut = (Future) futureItr.next();
          SingleHopOperationCallable task = (SingleHopOperationCallable) taskItr.next();
          ServerLocation server = task.getServer();
          try {
            VersionedObjectList versions = (VersionedObjectList) fut.get();
            if (logger.isDebugEnabled()) {
              logger.debug("submitBulkOp#got result from {}:{}", server, versions);
            }
            resultMap.put(server, versions);
          } catch (InterruptedException e) {
            InternalGemFireException ige = new InternalGemFireException(e);
            // only to make this server as failed server, not to throw right now
            failedServers.put(server, ige);
            if (rte == null) {
              rte = ige;
            }
          } catch (ExecutionException ee) {
            if (ee.getCause() instanceof ServerOperationException) {
              if (logger.isDebugEnabled()) {
                logger.debug("submitBulkOp#ExecutionException from server {}", server, ee);
              }
              ServerOperationException soe = (ServerOperationException) ee.getCause();
              // only to make this server as failed server, not to throw right now
              failedServers.put(server, soe);
              if (rte == null) {
                rte = soe;
              }
            } else if (ee.getCause() instanceof ServerConnectivityException) {
              if (logger.isDebugEnabled()) {
                logger.debug("submitBulkOp#ExecutionException for server {}", server, ee);
              }
              cms = region.getCache().getClientMetadataService();
              cms.removeBucketServerLocation(server);
              cms.scheduleGetPRMetaData(region, false);
              failedServers.put(server, (ServerConnectivityException) ee.getCause());
            } else {
              Throwable t = ee.getCause();
              if (t instanceof PutAllPartialResultException) {
                resultMap.put(server, t);
                anyPartialResults = true;
                failedServers.put(server, (PutAllPartialResultException) t);
              } else {
                RuntimeException other_rte = executionThrowable(ee.getCause());
                failedServers.put(server, other_rte);
                if (rte == null) {
                  rte = other_rte;
                }
              }
            }
          } // catch
        } // while
        // if there are any partial results we suppress throwing an exception
        // so the partial results can be processed
        if (rte != null && !anyPartialResults) {
          throw rte;
        }
      }
      return resultMap;
    }
    return null;
  }

  static Map<ServerLocation, Object> submitGetAll(Map<ServerLocation, HashSet> serverToFilterMap,
      List callableTasks, ClientMetadataService cms, LocalRegion region) {

    if (callableTasks != null && !callableTasks.isEmpty()) {
      Map<ServerLocation, Object> resultMap = new HashMap<ServerLocation, Object>();
      List futures = null;
      try {
        futures = execService.invokeAll(callableTasks);
      } catch (RejectedExecutionException rejectedExecutionEx) {
        throw rejectedExecutionEx;
      } catch (InterruptedException e) {
        throw new InternalGemFireException(e.getMessage());
      }
      if (futures != null) {
        Iterator futureItr = futures.iterator();
        Iterator taskItr = callableTasks.iterator();
        while (futureItr.hasNext() && !execService.isShutdown() && !execService.isTerminated()) {
          Future fut = (Future) futureItr.next();
          SingleHopOperationCallable task = (SingleHopOperationCallable) taskItr.next();
          List keys = ((GetAllOpImpl) task.getOperation()).getKeyList();
          ServerLocation server = task.getServer();
          try {

            VersionedObjectList valuesFromServer = (VersionedObjectList) fut.get();
            valuesFromServer.setKeys(keys);

            for (VersionedObjectList.Iterator it = valuesFromServer.iterator(); it.hasNext();) {
              VersionedObjectList.Entry entry = it.next();
              Object key = entry.getKey();
              Object value = entry.getValue();
              if (!entry.isKeyNotOnServer()) {
                if (value instanceof Throwable) {
                  logger.warn(LocalizedMessage.create(
                      LocalizedStrings.GetAll_0_CAUGHT_THE_FOLLOWING_EXCEPTION_ATTEMPTING_TO_GET_VALUE_FOR_KEY_1,
                      new Object[] {value, key}), (Throwable) value);
                }
              }
            }
            if (logger.isDebugEnabled()) {
              logger.debug("GetAllOp#got result from {}: {}", server, valuesFromServer);
            }
            resultMap.put(server, valuesFromServer);
          } catch (InterruptedException e) {
            throw new InternalGemFireException(e.getMessage());
          } catch (ExecutionException ee) {
            if (ee.getCause() instanceof ServerOperationException) {
              if (logger.isDebugEnabled()) {
                logger.debug("GetAllOp#ExecutionException.ServerOperationException : Caused by :{}",
                    ee.getCause());
              }
              throw (ServerOperationException) ee.getCause();
            } else if (ee.getCause() instanceof ServerConnectivityException) {
              if (logger.isDebugEnabled()) {
                logger.debug(
                    "GetAllOp#ExecutionException.ServerConnectivityException : Caused by :{} The failed server is: {}",
                    ee.getCause(), server);
              }
              try {
                cms = region.getCache().getClientMetadataService();
              } catch (CacheClosedException e) {
                return null;
              }
              cms.removeBucketServerLocation(server);
              cms.scheduleGetPRMetaData((LocalRegion) region, false);
              resultMap.put(server, ee.getCause());
            } else {
              throw executionThrowable(ee.getCause());
            }
          }
        }
        return resultMap;
      }
    }
    return null;
  }

  static void submitTask(Runnable task) {
    execService.submit(task);
  }

  // Find out what exception to throw?
  private static RuntimeException executionThrowable(Throwable t) {
    if (t instanceof RuntimeException)
      return (RuntimeException) t;
    else if (t instanceof Error)
      throw (Error) t;
    else
      throw new IllegalStateException("Don't know", t);
  }
}
