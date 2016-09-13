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
package com.gemstone.gemfire.distributed.internal.deadlock;

import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.execute.AbstractExecution;

/**
 * This class uses gemfire function execution to get the dependencies between
 * threads present in each member of the distributed system. It then uses the
 * {@link DeadlockDetector} class to determine if any deadlocks exist within
 * those dependencies.
 * 
 * 
 */
public class GemFireDeadlockDetector {

  private Set<DistributedMember> targetMembers = null;
  
  public GemFireDeadlockDetector() {
    
  }
  public GemFireDeadlockDetector (Set<DistributedMember> targetMembers) {
    this.targetMembers = targetMembers;
  }
  /**
   * Find any deadlocks the exist in this distributed system.
   * 
   * The deadlocks are returned as a list of dependencies. See {@link DeadlockDetector}
   */
  public DependencyGraph find() {
    
    final DeadlockDetector detector = new DeadlockDetector();
    ResultCollector<HashSet<Dependency>, Serializable> collector = new ResultCollector<HashSet<Dependency>, Serializable>() {

      public synchronized Serializable getResult()
          throws FunctionException {
        return null;
      }

      public synchronized Serializable getResult(long timeout,
          TimeUnit unit) throws FunctionException, InterruptedException {
        return null;
      }

      public synchronized void addResult(DistributedMember memberID,
          HashSet<Dependency> resultOfSingleExecution) {
        detector.addDependencies(resultOfSingleExecution);
        
      }

      public void endResults() {
        
      }

      public void clearResults() {
        
      }


    };
    
    Execution execution;
    if (targetMembers != null) {
      execution =  FunctionService.onMembers(targetMembers).withCollector(collector);
    } else {
      execution =  FunctionService.onMembers().withCollector(collector);
    }
    
    ((AbstractExecution) execution).setIgnoreDepartedMembers(true);
    collector = (ResultCollector<HashSet<Dependency>, Serializable>)execution.execute(new CollectDependencyFunction());
    
    //Wait for results
    collector.getResult();

    return detector.getDependencyGraph();
  }
  
  private static class CollectDependencyFunction implements Function {

    private static final long serialVersionUID = 6204378622627095817L;

    public boolean hasResult() {
      return true;
    }

    public void execute(FunctionContext context) {
      InternalDistributedSystem instance = InternalDistributedSystem.getAnyInstance();
      if(instance == null) {
        context.getResultSender().lastResult(new HashSet());
        return;
      }
      
      InternalDistributedMember member = instance.getDistributedMember();
      
      Set<Dependency> dependencies = DeadlockDetector.collectAllDependencies(member);
      context.getResultSender().lastResult((Serializable) dependencies);
    }

    public String getId() {
      return "DetectDeadlock";
    }

    public boolean optimizeForWrite() {
      return false;
    }

    public boolean isHA() {
      return false;
    }
  }
}
