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
package org.apache.geode.management;

import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

/**
 * MBean that provides access to an {@link AsyncEventQueue}.
 * 
 * @since GemFire 7.0
 * 
 */
@ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
public interface FunctionStatsMXBean {

  /**
   * Returns the ID of the Function.
   */
  public String getId();

  public int getFunctionExecutionsExceptions();

  public int getFunctionExecutionsHasResultRunning();

  public int getFunctionExecutionsHasResultCompletedProcessingTime();

  public int getFunctionExecutionCalls();

  public int getResultsSentToResultCollector();

  public int getFunctionExecutionsRunning();

  public long getFunctionExecutionsCompletedProcessingTime();

  public int getFunctionExecutionsCompleted();

  public int getResultsReceived();

}
