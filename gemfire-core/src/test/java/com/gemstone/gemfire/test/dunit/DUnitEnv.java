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
/**
 * 
 */
package com.gemstone.gemfire.test.dunit;

import java.io.File;
import java.rmi.RemoteException;
import java.util.Properties;

import com.gemstone.gemfire.test.dunit.standalone.BounceResult;

/**
 * This class provides an abstraction over the environment
 * that is used to run dunit. This will delegate to the hydra
 * or to the standalone dunit launcher as needed.
 * 
 * Any dunit tests that rely on hydra configuration should go
 * through here, so that we can separate them out from depending on hydra
 * and run them on a different VM launching system.
 *   
 * @author dsmith
 */
public abstract class DUnitEnv {
  
  public static DUnitEnv instance = null;
  
  public static final DUnitEnv get() {
    if (instance == null) {
      try {
        // for tests that are still being migrated to the open-source
        // distributed unit test framework  we need to look for this
        // old closed-source dunit environment
        Class clazz = Class.forName("dunit.hydra.HydraDUnitEnv");
        instance = (DUnitEnv)clazz.newInstance();
      } catch (Exception e) {
        throw new Error("Distributed unit test environment is not initialized");
      }
    }
    return instance;
  }
  
  public static void set(DUnitEnv dunitEnv) {
    instance = dunitEnv;
  }
  
  public abstract String getLocatorString();
  
  public abstract String getLocatorAddress();

  public abstract int getLocatorPort();
  
  public abstract Properties getDistributedSystemProperties();

  public abstract int getPid();

  public abstract int getVMID();

  public abstract BounceResult bounce(int pid) throws RemoteException;

  public abstract File getWorkingDirectory(int pid);
  
}
