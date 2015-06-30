/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package dunit.standalone;

import hydra.MethExecutor;
import hydra.MethExecutorResult;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.internal.logging.LogService;

import dunit.RemoteDUnitVMIF;

/**
 * @author dsmith
 *
 */
public class RemoteDUnitVM extends UnicastRemoteObject implements RemoteDUnitVMIF {
  
  private static final Logger logger = LogService.getLogger();
  
  public RemoteDUnitVM() throws RemoteException {
    super();
  }

  /** 
   * Called remotely by the master controller to cause the client to execute 
   * the instance method on the object.  Does this synchronously (does not spawn
   * a thread).  This method is used by the unit test framework, dunit.
   *
   * @param obj the object to execute the method on
   * @param methodName the name of the method to execute
   * @return the result of method execution
   */ 
   public MethExecutorResult executeMethodOnObject( Object obj, String methodName ) {
     String name = obj.getClass().getName() + "." + methodName + 
       " on object: " + obj;
     logger.info("Received method: " + name);
     long start = System.currentTimeMillis();
     MethExecutorResult result = MethExecutor.executeObject( obj, methodName );
     long delta = System.currentTimeMillis() - start;
     logger.info( "Got result: " + result.toString().trim()  + " from " +
               name + " (took " + delta + " ms)");
     return result;
   }

   /**
    * Executes a given instance method on a given object with the given
    * arguments. 
    */
   public MethExecutorResult executeMethodOnObject(Object obj,
                                                   String methodName,
                                                   Object[] args) {
     String name = obj.getClass().getName() + "." + methodName + 
              (args != null ? " with " + args.length + " args": "") +
       " on object: " + obj;
     logger.info("Received method: " + name);
     long start = System.currentTimeMillis();
     MethExecutorResult result = 
       MethExecutor.executeObject(obj, methodName, args);
     long delta = System.currentTimeMillis() - start;
     logger.info( "Got result: " + result.toString() + " from " + name + 
               " (took " + delta + " ms)");
     return result;
   }

  /** 
   * Called remotely by the master controller to cause the client to execute 
   * the method on the class.  Does this synchronously (does not spawn a thread).
   * This method is used by the unit test framework, dunit.
   *
   * @param className the name of the class execute
   * @param methodName the name of the method to execute
   * @return the result of method execution
   */ 
   public MethExecutorResult executeMethodOnClass( String className, String methodName ) {
     String name = className + "." + methodName;
     logger.info("Received method: " +  name);
     long start = System.currentTimeMillis();
     MethExecutorResult result = MethExecutor.execute( className, methodName );
     long delta = System.currentTimeMillis() - start;
     logger.info( "Got result: " + result.toString() + " from " + name + 
               " (took " + delta + " ms)");
     
     return result;
   }

   /**
    * Executes a given static method in a given class with the given
    * arguments. 
    */
   public MethExecutorResult executeMethodOnClass(String className,
                                                  String methodName,
                                                  Object[] args) {
     String name = className + "." + methodName + 
       (args != null ? " with " + args.length + " args": "");
     logger.info("Received method: " + name);
     long start = System.currentTimeMillis();
     MethExecutorResult result = 
       MethExecutor.execute(className, methodName, args);
     long delta = System.currentTimeMillis() - start;
     logger.info( "Got result: " + result.toString() + " from " + name +
               " (took " + delta + " ms)");
     return result;
   }

  public void executeTask(int tsid, int type, int index) throws RemoteException {
    throw new UnsupportedOperationException();
    
  }
  
  public void runShutdownHook() throws RemoteException {
    
  }

  public void notifyDynamicActionComplete(int actionId) throws RemoteException {
    throw new UnsupportedOperationException();
    
  }

  public void shutDownVM(boolean disconnect, boolean runShutdownHook)
      throws RemoteException {
  }

  public void disconnectVM()
  throws RemoteException {
  }
}
