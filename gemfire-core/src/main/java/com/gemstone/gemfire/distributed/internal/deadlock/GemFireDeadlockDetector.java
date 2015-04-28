/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
 * @author dsmith
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
