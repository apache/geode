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

package org.apache.geode.internal.cache.execute;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.Logger;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.LowMemoryException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.client.internal.ProxyCache;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.logging.LogService;

/**
 * Abstract implementation of InternalExecution interface.
 *
 * @since GemFire 5.8LA
 *
 */
public abstract class AbstractExecution implements InternalExecution {

  private static final Logger logger = LogService.getLogger();

  protected boolean isMemberMappedArgument;

  protected MemberMappedArgument memberMappedArg;

  protected Object args;

  protected ResultCollector rc;

  protected Set filter = new HashSet();

  protected boolean hasRoutingObjects;

  protected volatile boolean isReExecute = false;

  protected volatile boolean isClientServerMode = false;

  protected Set<String> failedNodes = new HashSet<String>();

  protected boolean isFnSerializationReqd;

  /***
   * yjing The following code is added to get a set of function executing nodes by the data aware
   * procedure
   */
  protected Collection<InternalDistributedMember> executionNodes = null;

  public interface ExecutionNodesListener {

    void afterExecutionNodesSet(AbstractExecution execution);

    void reset();
  }

  protected ExecutionNodesListener executionNodesListener = null;

  protected boolean waitOnException = false;

  protected boolean forwardExceptions = false;

  protected boolean ignoreDepartedMembers = false;

  protected ProxyCache proxyCache;

  private static final ConcurrentHashMap<String, byte[]> idToFunctionAttributes =
      new ConcurrentHashMap<String, byte[]>();

  public static final byte NO_HA_NO_HASRESULT_NO_OPTIMIZEFORWRITE = 0;

  public static final byte NO_HA_HASRESULT_NO_OPTIMIZEFORWRITE = 2;

  public static final byte HA_HASRESULT_NO_OPTIMIZEFORWRITE = 3;

  public static final byte NO_HA_NO_HASRESULT_OPTIMIZEFORWRITE = 4;

  public static final byte NO_HA_HASRESULT_OPTIMIZEFORWRITE = 6;

  public static final byte HA_HASRESULT_OPTIMIZEFORWRITE = 7;

  public static final byte HA_HASRESULT_NO_OPTIMIZEFORWRITE_REEXECUTE = 11;

  public static final byte HA_HASRESULT_OPTIMIZEFORWRITE_REEXECUTE = 15;

  public static byte getFunctionState(boolean isHA, boolean hasResult, boolean optimizeForWrite) {
    if (isHA) {
      if (hasResult) {
        if (optimizeForWrite) {
          return HA_HASRESULT_OPTIMIZEFORWRITE;
        } else {
          return HA_HASRESULT_NO_OPTIMIZEFORWRITE;
        }
      }
      return (byte) 1; // ERROR scenario
    } else {
      if (hasResult) {
        if (optimizeForWrite) {
          return NO_HA_HASRESULT_OPTIMIZEFORWRITE;
        } else {
          return NO_HA_HASRESULT_NO_OPTIMIZEFORWRITE;
        }
      } else {
        if (optimizeForWrite) {
          return NO_HA_NO_HASRESULT_OPTIMIZEFORWRITE;
        } else {
          return NO_HA_NO_HASRESULT_NO_OPTIMIZEFORWRITE;
        }
      }
    }
  }

  public static byte getReexecuteFunctionState(byte fnState) {
    if (fnState == HA_HASRESULT_NO_OPTIMIZEFORWRITE) {
      return HA_HASRESULT_NO_OPTIMIZEFORWRITE_REEXECUTE;
    } else if (fnState == HA_HASRESULT_OPTIMIZEFORWRITE) {
      return HA_HASRESULT_OPTIMIZEFORWRITE_REEXECUTE;
    }
    throw new InternalGemFireException("Wrong fnState provided.");
  }

  protected AbstractExecution() {}

  protected AbstractExecution(AbstractExecution ae) {
    if (ae.args != null) {
      this.args = ae.args;
    }
    if (ae.rc != null) {
      this.rc = ae.rc;
    }
    if (ae.memberMappedArg != null) {
      this.memberMappedArg = ae.memberMappedArg;
    }
    this.isMemberMappedArgument = ae.isMemberMappedArgument;
    this.isClientServerMode = ae.isClientServerMode;
    if (ae.proxyCache != null) {
      this.proxyCache = ae.proxyCache;
    }
    this.isFnSerializationReqd = ae.isFnSerializationReqd;
  }

  protected AbstractExecution(AbstractExecution ae, boolean isReExecute) {
    this(ae);
    this.isReExecute = isReExecute;
  }

  public boolean isMemberMappedArgument() {
    return this.isMemberMappedArgument;
  }

  public Object getArgumentsForMember(String memberId) {
    if (!isMemberMappedArgument) {
      return this.args;
    } else {
      return this.memberMappedArg.getArgumentsForMember(memberId);
    }
  }

  public MemberMappedArgument getMemberMappedArgument() {
    return this.memberMappedArg;
  }

  public Object getArguments() {
    return this.args;
  }

  public ResultCollector getResultCollector() {
    return this.rc;
  }

  public Set getFilter() {
    return this.filter;
  }

  public AbstractExecution setIsReExecute() {
    this.isReExecute = true;
    if (this.executionNodesListener != null) {
      this.executionNodesListener.reset();
    }
    return this;
  }

  public boolean isReExecute() {
    return isReExecute;
  }

  public Set<String> getFailedNodes() {
    return this.failedNodes;
  }

  public void addFailedNode(String failedNode) {
    this.failedNodes.add(failedNode);
  }

  public void clearFailedNodes() {
    this.failedNodes.clear();
  }

  public boolean isClientServerMode() {
    return isClientServerMode;
  }

  public boolean isFnSerializationReqd() {
    return isFnSerializationReqd;
  }

  public Collection<InternalDistributedMember> getExecutionNodes() {
    return this.executionNodes;
  }

  public void setRequireExecutionNodes(ExecutionNodesListener listener) {
    this.executionNodes = Collections.emptySet();
    this.executionNodesListener = listener;
  }

  public void setExecutionNodes(Set<InternalDistributedMember> nodes) {
    if (this.executionNodes != null) {
      this.executionNodes = nodes;
      if (this.executionNodesListener != null) {
        this.executionNodesListener.afterExecutionNodesSet(this);
      }
    }
  }

  public void executeFunctionOnLocalPRNode(final Function fn, final FunctionContext cx,
      final PartitionedRegionFunctionResultSender sender, DistributionManager dm, boolean isTx) {
    if (dm instanceof ClusterDistributionManager && !isTx) {
      if (ServerConnection.isExecuteFunctionOnLocalNodeOnly().byteValue() == 1) {
        ServerConnection.executeFunctionOnLocalNodeOnly((byte) 3);// executed locally
        executeFunctionLocally(fn, cx, sender, dm);
        if (!sender.isLastResultReceived() && fn.hasResult()) {
          ((InternalResultSender) sender).setException(new FunctionException(
              String.format("The function, %s, did not send last result",
                  fn.getId())));
        }
      } else {

        final ClusterDistributionManager newDM = (ClusterDistributionManager) dm;
        newDM.getFunctionExecutor().execute(new Runnable() {
          public void run() {
            executeFunctionLocally(fn, cx, sender, newDM);
            if (!sender.isLastResultReceived() && fn.hasResult()) {
              ((InternalResultSender) sender).setException(new FunctionException(
                  String.format("The function, %s, did not send last result",
                      fn.getId())));
            }
          }
        });
      }
    } else {
      executeFunctionLocally(fn, cx, sender, dm);
      if (!sender.isLastResultReceived() && fn.hasResult()) {
        ((InternalResultSender) sender).setException(new FunctionException(
            String.format("The function, %s, did not send last result",
                fn.getId())));
      }
    }
  }

  // Bug41118 : in case of lonerDistribuedSystem do local execution through
  // main thread otherwise give execution to FunctionExecutor from
  // DistributionManager
  public void executeFunctionOnLocalNode(final Function<?> fn, final FunctionContext cx,
      final ResultSender sender, DistributionManager dm, final boolean isTx) {
    if (dm instanceof ClusterDistributionManager && !isTx) {
      final ClusterDistributionManager newDM = (ClusterDistributionManager) dm;
      newDM.getFunctionExecutor().execute(new Runnable() {
        public void run() {
          executeFunctionLocally(fn, cx, sender, newDM);
          if (!((InternalResultSender) sender).isLastResultReceived() && fn.hasResult()) {
            ((InternalResultSender) sender).setException(new FunctionException(
                String.format("The function, %s, did not send last result",
                    fn.getId())));
          }
        }
      });
    } else {
      executeFunctionLocally(fn, cx, sender, dm);
      if (!((InternalResultSender) sender).isLastResultReceived() && fn.hasResult()) {
        ((InternalResultSender) sender).setException(new FunctionException(
            String.format("The function, %s, did not send last result",
                fn.getId())));
      }
    }
  }

  public void executeFunctionLocally(final Function<?> fn, final FunctionContext cx,
      final ResultSender sender, DistributionManager dm) {

    FunctionStats stats = FunctionStats.getFunctionStats(fn.getId(), dm.getSystem());

    try {
      long start = stats.startTime();
      stats.startFunctionExecution(fn.hasResult());
      if (logger.isDebugEnabled()) {
        logger.debug("Executing Function: {} on local node with context: {}", fn.getId(),
            cx.toString());
      }

      fn.execute(cx);
      stats.endFunctionExecution(start, fn.hasResult());
    } catch (FunctionInvocationTargetException fite) {
      FunctionException functionException = null;
      if (fn.isHA()) {
        functionException =
            new FunctionException(new InternalFunctionInvocationTargetException(fite.getMessage()));
      } else {
        functionException = new FunctionException(fite);
      }
      handleException(functionException, fn, cx, sender, dm);
    } catch (BucketMovedException bme) {
      FunctionException functionException = null;
      if (fn.isHA()) {
        functionException =
            new FunctionException(new InternalFunctionInvocationTargetException(bme));
      } else {
        functionException = new FunctionException(bme);
      }
      handleException(functionException, fn, cx, sender, dm);
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      handleException(t, fn, cx, sender, dm);
    }
  }

  public ResultCollector execute(final String functionName) {
    if (functionName == null) {
      throw new FunctionException(
          "The input function for the execute function request is null");
    }
    this.isFnSerializationReqd = false;
    Function functionObject = FunctionService.getFunction(functionName);
    if (functionObject == null) {
      throw new FunctionException(
          String.format("Function named %s is not registered to FunctionService",
              functionName));
    }

    return executeFunction(functionObject);
  }

  public ResultCollector execute(Function function) throws FunctionException {
    if (function == null) {
      throw new FunctionException(
          "The input function for the execute function request is null");
    }
    if (function.isHA() && !function.hasResult()) {
      throw new FunctionException(
          "For Functions with isHA true, hasResult must also be true.");
    }

    String id = function.getId();
    if (id == null) {
      throw new IllegalArgumentException(
          "The Function#getID() returned null");
    }
    this.isFnSerializationReqd = true;
    return executeFunction(function);
  }

  public void setWaitOnExceptionFlag(boolean waitOnException) {
    this.setForwardExceptions(waitOnException);
    this.waitOnException = waitOnException;
  }

  public boolean getWaitOnExceptionFlag() {
    return this.waitOnException;
  }

  public void setForwardExceptions(boolean forward) {
    this.forwardExceptions = forward;
  }

  public boolean isForwardExceptions() {
    return forwardExceptions;
  }

  @Override
  public void setIgnoreDepartedMembers(boolean ignore) {
    this.ignoreDepartedMembers = ignore;
    if (ignore) {
      setWaitOnExceptionFlag(true);
    }
  }

  public boolean isIgnoreDepartedMembers() {
    return this.ignoreDepartedMembers;
  }

  protected abstract ResultCollector executeFunction(Function fn);

  /**
   * validates whether a function should execute in presence of transaction and HeapCritical
   * members. If the function is the first operation in a transaction, bootstraps the function.
   *
   * @param function the function
   * @param targetMembers the set of members the function will be executed on
   * @throws TransactionException if more than one nodes are targeted within a transaction
   * @throws LowMemoryException if the set contains a heap critical member
   */
  public abstract void validateExecution(Function function, Set targetMembers);

  public LocalResultCollector<?, ?> getLocalResultCollector(Function function,
      final ResultCollector<?, ?> rc) {
    if (rc instanceof LocalResultCollector) {
      return (LocalResultCollector) rc;
    } else {
      return new LocalResultCollectorImpl(function, rc, this);
    }
  }

  /**
   * Returns the function attributes defined by the functionId, returns null if no function is found
   * for the specified functionId
   *
   * @return byte[]
   * @throws FunctionException if functionID passed is null
   * @since GemFire 6.6
   */
  public byte[] getFunctionAttributes(String functionId) {
    if (functionId == null) {
      throw new FunctionException(String.format("%s passed is null",
          "functionId instance "));
    }
    return idToFunctionAttributes.get(functionId);
  }

  public void removeFunctionAttributes(String functionId) {
    idToFunctionAttributes.remove(functionId);
  }

  public void addFunctionAttributes(String functionId, byte[] functionAttributes) {
    idToFunctionAttributes.put(functionId, functionAttributes);
  }

  private void handleException(Throwable functionException, final Function fn,
      final FunctionContext cx, final ResultSender sender, DistributionManager dm) {
    FunctionStats stats = FunctionStats.getFunctionStats(fn.getId(), dm.getSystem());

    if (logger.isDebugEnabled()) {
      logger.debug("Exception occurred on local node while executing Function: {}", fn.getId(),
          functionException);
    }
    stats.endFunctionExecutionWithException(fn.hasResult());
    if (fn.hasResult()) {
      if (waitOnException || forwardExceptions) {
        if (functionException instanceof FunctionException
            && functionException.getCause() instanceof QueryInvalidException) {
          // Handle this exception differently since it can contain
          // non-serializable objects.
          // java.io.NotSerializableException: antlr.CommonToken
          // create a new FunctionException on the original one's message (not cause).
          functionException = new FunctionException(functionException.getLocalizedMessage());
        }
        sender.lastResult(functionException);
      } else {
        ((InternalResultSender) sender).setException(functionException);
      }
    } else {
      logger.warn("Exception occurred on local node while executing Function:",
          functionException);
    }
  }
}
