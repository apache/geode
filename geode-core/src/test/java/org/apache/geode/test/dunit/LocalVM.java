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

package org.apache.geode.test.dunit;

import java.rmi.RemoteException;

import hydra.MethExecutor;
import hydra.MethExecutorResult;

import org.apache.geode.test.dunit.standalone.RemoteDUnitVMIF;
import org.apache.geode.test.dunit.standalone.VersionManager;

public class LocalVM extends VM {

  public LocalVM(final VM vm) {
    this(vm.getHost(), vm.getId());
  }

  public LocalVM(final Host host, int id) {
    this(host, VersionManager.CURRENT_VERSION, id);
  }

  public LocalVM(final Host host, final String version, final int id) {
    super(host, version, id, new LocalDUnitVMIF());
  }

  public Object invoke(final Class targetClass, final String methodName, final Object[] args) {
    MethExecutorResult result = execute(targetClass.getName(), methodName, args);

    if (!result.exceptionOccurred()) {
      return result.getResult();

    } else {
      throw new RMIException(this, targetClass.getName(), methodName, result.getException(),
          result.getStackTrace());
    }
  }

  public void bounce(final String targetVersion) {
    throwUnsupportedOperationException();
  }

  protected static MethExecutorResult execute(final String targetClass, final String methodName,
      final Object[] args) {
    return MethExecutor.execute(targetClass, methodName, args);
  }

  protected static void throwUnsupportedOperationException() {
    throw new UnsupportedOperationException("Not supported by LocalVM");
  }

  protected static class LocalDUnitVMIF implements RemoteDUnitVMIF {

    @Override
    public MethExecutorResult executeMethodOnObject(final Object target, final String methodName)
        throws RemoteException {
      return MethExecutor.executeObject(target, methodName, new Object[0]);
    }

    @Override
    public MethExecutorResult executeMethodOnObject(final Object target, final String methodName,
        final Object[] args) throws RemoteException {
      return MethExecutor.executeObject(target, methodName, args);
    }

    @Override
    public MethExecutorResult executeMethodOnClass(final String className, final String methodName,
        final Object[] args) throws RemoteException {
      return MethExecutor.executeObject(className, methodName, args);
    }

    @Override
    public void shutDownVM() throws RemoteException {
      throwUnsupportedOperationException();
    }
  }
}
