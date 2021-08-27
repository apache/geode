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
package org.apache.geode.test.dunit.internal;


import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import org.apache.geode.logging.internal.log4j.api.LogService;

class RemoteDUnitVM extends UnicastRemoteObject implements RemoteDUnitVMIF {
  private static final Logger logger = LogService.getLogger();

  RemoteDUnitVM() throws RemoteException {
    // super
  }

  /**
   * Called remotely by the master controller to cause the client to execute the instance method on
   * the object. Does this synchronously (does not spawn a thread). This method is used by the unit
   * test framework, dunit.
   *
   * @param target the object to execute the method on
   * @param methodName the name of the method to execute
   * @return the result of method execution
   */
  @Override
  public MethodInvokerResult executeMethodOnObject(Object target, String methodName) {
    String name = target.getClass().getName() + '.' + methodName + " on object: " + target;
    long start = start(name);
    MethodInvokerResult result = MethodInvoker.executeObject(target, methodName);
    logDelta(name, start, result);
    return result;
  }

  protected long start(String name) {
    logger.debug("Received method: {}", name);
    return System.nanoTime();
  }

  private void logDelta(String name, long start, MethodInvokerResult result) {
    long delta = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    logger.debug("Got result: {} from {} (took {} ms)", result, name, delta);
  }

  /**
   * Executes a given instance method on a given object with the given arguments.
   */
  @Override
  public MethodInvokerResult executeMethodOnObject(Object target, String methodName,
      Object[] args) {
    long asyncId = 0;
    if (target instanceof Identifiable) {
      asyncId = ((Identifiable) target).getId();
      if (asyncId > 0) {
        AsyncThreadId.put(asyncId, Thread.currentThread().getId());
      }
    }
    try {
      String name = target.getClass().getName() + '.' + methodName
          + (args != null ? " with " + args.length + " args" : "") + " on object: " + target;
      long start = start(name);
      MethodInvokerResult result = MethodInvoker.executeObject(target, methodName, args);
      logDelta(name, start, result);
      return result;
    } finally {
      if (asyncId != 0) {
        AsyncThreadId.remove(asyncId);
      }
    }
  }

  /**
   * Called remotely by the master controller to cause the client to execute the method on the
   * class. Does this synchronously (does not spawn a thread). This method is used by the unit test
   * framework, dunit.
   *
   * @param className the name of the class execute
   * @param methodName the name of the method to execute
   * @return the result of method execution
   */
  public MethodInvokerResult executeMethodOnClass(String className, String methodName) {
    String name = className + '.' + methodName;
    long start = start(name);
    MethodInvokerResult result = MethodInvoker.execute(className, methodName);
    logDelta(name, start, result);

    return result;
  }

  /**
   * Executes a given static method in a given class with the given arguments.
   */
  @Override
  public MethodInvokerResult executeMethodOnClass(String className, String methodName,
      Object[] args) {
    String name =
        className + '.' + methodName + (args != null ? " with " + args.length + " args" : "");
    long start = start(name);
    MethodInvokerResult result = MethodInvoker.execute(className, methodName, args);
    logDelta(name, start, result);
    return result;
  }

  public void executeTask(int tsid, int type, int index) throws RemoteException {
    throw new UnsupportedOperationException("executeTask is not implemented");
  }

  public void runShutdownHook() throws RemoteException {
    throw new UnsupportedOperationException("runShutdownHook is not implemented");
  }

  public void notifyDynamicActionComplete(int actionId) throws RemoteException {
    throw new UnsupportedOperationException("notifyDynamicActionComplete is not implemented");
  }

  @Override
  public void shutDownVM() throws RemoteException {
    ChildVM.stopVM();
  }

  public void disconnectVM() throws RemoteException {
    throw new UnsupportedOperationException("disconnectVM is not implemented");
  }

  private static final long serialVersionUID = 251934856609958734L;
}
