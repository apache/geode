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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.Logger;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.internal.MakeNotStatic;
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
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionConfig;
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

  public static final int DEFAULT_CLIENT_FUNCTION_TIMEOUT = 0;
  private static final String CLIENT_FUNCTION_TIMEOUT_SYSTEM_PROPERTY =
      DistributionConfig.GEMFIRE_PREFIX + "CLIENT_FUNCTION_TIMEOUT";
  private static final Integer timeoutMsSystemProperty =
      Integer.getInteger(CLIENT_FUNCTION_TIMEOUT_SYSTEM_PROPERTY, DEFAULT_CLIENT_FUNCTION_TIMEOUT);

  boolean isMemberMappedArgument;

  protected MemberMappedArgument memberMappedArg;

  protected Object args;

  protected ResultCollector rc;

  protected Set filter = new HashSet();

  protected volatile boolean isReExecute = false;

  volatile boolean isClientServerMode = false;

  Set<String> failedNodes = new HashSet<>();

  protected boolean isFnSerializationReqd;

  /***
   * yjing The following code is added to get a set of function executing nodes by the data aware
   * procedure
   */
  private Collection<InternalDistributedMember> executionNodes = null;

  public interface ExecutionNodesListener {

    void afterExecutionNodesSet(AbstractExecution execution);

    void reset();
  }

  private ExecutionNodesListener executionNodesListener = null;

  boolean waitOnException = false;

  boolean forwardExceptions = false;

  private boolean ignoreDepartedMembers = false;

  protected ProxyCache proxyCache;

  private final int timeoutMs;

  @MakeNotStatic
  private static final ConcurrentHashMap<String, byte[]> idToFunctionAttributes =
      new ConcurrentHashMap<>();

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

  protected AbstractExecution() {
    timeoutMs =
        timeoutMsSystemProperty >= 0 ? timeoutMsSystemProperty : DEFAULT_CLIENT_FUNCTION_TIMEOUT;
  }

  protected AbstractExecution(AbstractExecution ae) {
    if (ae.args != null) {
      args = ae.args;
    }
    if (ae.rc != null) {
      rc = ae.rc;
    }
    if (ae.memberMappedArg != null) {
      memberMappedArg = ae.memberMappedArg;
    }
    isMemberMappedArgument = ae.isMemberMappedArgument;
    isClientServerMode = ae.isClientServerMode;
    if (ae.proxyCache != null) {
      proxyCache = ae.proxyCache;
    }
    isFnSerializationReqd = ae.isFnSerializationReqd;
    timeoutMs = ae.timeoutMs;
  }

  protected AbstractExecution(AbstractExecution ae, boolean isReExecute) {
    this(ae);
    this.isReExecute = isReExecute;
  }

  public Object getArgumentsForMember(String memberId) {
    if (!isMemberMappedArgument) {
      return args;
    } else {
      return memberMappedArg.getArgumentsForMember(memberId);
    }
  }

  public MemberMappedArgument getMemberMappedArgument() {
    return memberMappedArg;
  }

  public Object getArguments() {
    return args;
  }

  public ResultCollector getResultCollector() {
    return rc;
  }

  public Set getFilter() {
    return filter;
  }

  public AbstractExecution setIsReExecute() {
    isReExecute = true;
    if (executionNodesListener != null) {
      executionNodesListener.reset();
    }
    return this;
  }

  public boolean isReExecute() {
    return isReExecute;
  }

  public Set<String> getFailedNodes() {
    return failedNodes;
  }

  public void addFailedNode(String failedNode) {
    failedNodes.add(failedNode);
  }

  public void clearFailedNodes() {
    failedNodes.clear();
  }

  public boolean isClientServerMode() {
    return isClientServerMode;
  }

  public boolean isFnSerializationReqd() {
    return isFnSerializationReqd;
  }

  public void setExecutionNodes(Set<InternalDistributedMember> nodes) {
    if (executionNodes != null) {
      executionNodes = nodes;
      if (executionNodesListener != null) {
        executionNodesListener.afterExecutionNodesSet(this);
      }
    }
  }

  public void executeFunctionOnLocalPRNode(final Function fn, final FunctionContext cx,
      final PartitionedRegionFunctionResultSender sender, DistributionManager dm, boolean isTx) {
    if (dm instanceof ClusterDistributionManager && !isTx) {
      if (ServerConnection.isExecuteFunctionOnLocalNodeOnly() == 1) {
        ServerConnection.executeFunctionOnLocalNodeOnly((byte) 3);// executed locally
        executeFunctionLocally(fn, cx, sender, dm);
        if (!sender.isLastResultReceived() && fn.hasResult()) {
          ((InternalResultSender) sender).setException(new FunctionException(
              String.format("The function, %s, did not send last result",
                  fn.getId())));
        }
      } else {

        final ClusterDistributionManager newDM = (ClusterDistributionManager) dm;
        newDM.getExecutors().getFunctionExecutor().execute(() -> {
          executeFunctionLocally(fn, cx, sender, newDM);
          if (!sender.isLastResultReceived() && fn.hasResult()) {
            ((InternalResultSender) sender).setException(new FunctionException(
                String.format("The function, %s, did not send last result",
                    fn.getId())));
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
      newDM.getExecutors().getFunctionExecutor().execute(() -> {
        executeFunctionLocally(fn, cx, sender, newDM);
        if (!((InternalResultSender) sender).isLastResultReceived() && fn.hasResult()) {
          ((InternalResultSender) sender).setException(new FunctionException(
              String.format("The function, %s, did not send last result",
                  fn.getId())));
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

  private void executeFunctionLocally(final Function<?> fn, final FunctionContext cx,
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
      FunctionException functionException;
      if (fn.isHA()) {
        functionException =
            new FunctionException(new InternalFunctionInvocationTargetException(fite.getMessage()));
      } else {
        functionException = new FunctionException(fite);
      }
      handleException(functionException, fn, sender, dm);
    } catch (BucketMovedException bme) {
      FunctionException functionException;
      if (fn.isHA()) {
        functionException =
            new FunctionException(new InternalFunctionInvocationTargetException(bme));
      } else {
        functionException = new FunctionException(bme);
      }
      handleException(functionException, fn, sender, dm);
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      handleException(t, fn, sender, dm);
    }
  }

  @Override
  public ResultCollector execute(final String functionName) {
    if (functionName == null) {
      throw new FunctionException(
          "The input function for the execute function request is null");
    }
    isFnSerializationReqd = false;
    Function functionObject = FunctionService.getFunction(functionName);
    if (functionObject == null) {
      throw new FunctionException(
          String.format("Function named %s is not registered to FunctionService",
              functionName));
    }

    return executeFunction(functionObject);
  }

  @Override
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
    isFnSerializationReqd = true;
    return executeFunction(function);
  }

  @Override
  public void setWaitOnExceptionFlag(boolean waitOnException) {
    setForwardExceptions(waitOnException);
    this.waitOnException = waitOnException;
  }

  public boolean getWaitOnExceptionFlag() {
    return waitOnException;
  }

  @Override
  public void setForwardExceptions(boolean forward) {
    forwardExceptions = forward;
  }

  public boolean isForwardExceptions() {
    return forwardExceptions;
  }

  @Override
  public void setIgnoreDepartedMembers(boolean ignore) {
    ignoreDepartedMembers = ignore;
    if (ignore) {
      setWaitOnExceptionFlag(true);
    }
  }

  public boolean isIgnoreDepartedMembers() {
    return ignoreDepartedMembers;
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
  public abstract void validateExecution(Function function,
      Set<? extends DistributedMember> targetMembers);

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

  void addFunctionAttributes(String functionId, byte[] functionAttributes) {
    idToFunctionAttributes.put(functionId, functionAttributes);
  }

  private void handleException(Throwable functionException, final Function fn,
      final ResultSender sender, DistributionManager dm) {
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

  /**
   * Get function timeout in milliseconds.
   *
   * @return timeout in milliseconds.
   */
  int getTimeoutMs() {
    return timeoutMs;
  }
}
