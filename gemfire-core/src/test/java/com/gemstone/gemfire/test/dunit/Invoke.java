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
package com.gemstone.gemfire.test.dunit;

import java.util.HashMap;
import java.util.Map;

/**
 * <code>Invoke</code> provides static utility methods that allow a
 * <code>DistributedTest</code> to invoke a <code>SerializableRunnable</code>
 * or <code>SerializableCallable</code> in a remote test <code>VM</code>.
 * 
 * These methods can be used directly: <code>Invoke.invokeInEveryVM(...)</code>, 
 * however, they are intended to be referenced through static import:
 *
 * <pre>
 * import static com.gemstone.gemfire.test.dunit.Invoke.*;
 *    ...
 *    invokeInEveryVM(...);
 * </pre>
 *
 * Extracted from DistributedTestCase.
 */
public class Invoke {

  protected Invoke() {
  }
  
  /**
   * Invokes a <code>SerializableRunnable</code> in every VM that
   * DUnit knows about.
   * <p>
   * Note: this does NOT include the controller VM or locator VM.
   *
   * @see VM#invoke(SerializableRunnableIF)
   */
  public static void invokeInEveryVM(final SerializableRunnableIF runnable) {
    for (int hostIndex = 0; hostIndex < Host.getHostCount(); hostIndex++) {
      Host host = Host.getHost(hostIndex);
  
      for (int vmIndex = 0; vmIndex < host.getVMCount(); vmIndex++) {
        VM vm = host.getVM(vmIndex);
        vm.invoke(runnable);
      }
    }
  }

  /**
   * Invokes a method in every remote VM that DUnit knows about.
   *
   * @see VM#invoke(Class, String)
   * @deprecated Please use {@link #invokeInEveryVM(SerializableRunnableIF)} or another non-deprecated method in <code>Invoke</code> instead.
   */
  @Deprecated
  public static void invokeInEveryVM(final Class<?> targetClass, final String targetMethod) {
    for (int hostIndex = 0; hostIndex < Host.getHostCount(); hostIndex++) {
      Host host = Host.getHost(hostIndex);
  
      for (int vmIndex = 0; vmIndex < host.getVMCount(); vmIndex++) {
        VM vm = host.getVM(vmIndex);
        vm.invoke(targetClass, targetMethod);
      }
    }
  }

  /**
   * Invokes a method in every remote VM that DUnit knows about.
   *
   * @see VM#invoke(Class, String)
   * @deprecated Please use {@link #invokeInEveryVM(SerializableRunnableIF)} or another non-deprecated method in <code>Invoke</code> instead.
   */
  public static void invokeInEveryVM(final Class<?> targetClass, final String targetMethod, final Object[] methodArgs) {
    for (int hostIndex = 0; hostIndex < Host.getHostCount(); hostIndex++) {
      Host host = Host.getHost(hostIndex);
  
      for (int vmIndex = 0; vmIndex < host.getVMCount(); vmIndex++) {
        VM vm = host.getVM(vmIndex);
        vm.invoke(targetClass, targetMethod, methodArgs);
      }
    }
  }

  /**
   * Invokes a <code>SerializableCallable</code> in every VM that
   * DUnit knows about.
   *
   * @return a Map of results, where the key is the VM and the value is the result for that VM
   * @see VM#invoke(SerializableCallableIF)
   */
  public static <T> Map<VM, T> invokeInEveryVM(final SerializableCallableIF<T> callable) {
    Map<VM, T> ret = new HashMap<VM, T>();
    for (int h = 0; h < Host.getHostCount(); h++) {
      Host host = Host.getHost(h);
      for (int v = 0; v < host.getVMCount(); v++) {
        VM vm = host.getVM(v);
        ret.put(vm, vm.invoke(callable));
      }
    }
    return ret;
  }

  public static void invokeInLocator(final SerializableRunnableIF runnable) {
    Host.getLocator().invoke(runnable);
  }

  /**
   * @deprecated Please use {@link com.jayway.awaitility.Awaitility} with {@link #invokeInEveryVM(SerializableCallableIF)} instead.
   */
  public static void invokeRepeatingIfNecessary(final VM vm, final RepeatableRunnable runnable) {
    vm.invokeRepeatingIfNecessary(runnable, 0);
  }

  /**
   * @deprecated Please use {@link com.jayway.awaitility.Awaitility} with {@link #invokeInEveryVM(SerializableCallableIF)} instead.
   */
  public static void invokeRepeatingIfNecessary(final VM vm, final RepeatableRunnable runnable, final long repeatTimeoutMs) {
    vm.invokeRepeatingIfNecessary(runnable, repeatTimeoutMs);
  }

  /**
   * @deprecated Please use {@link com.jayway.awaitility.Awaitility} with {@link #invokeInEveryVM(SerializableCallableIF)} instead.
   */
  public static void invokeInEveryVMRepeatingIfNecessary(final RepeatableRunnable runnable) {
    Invoke.invokeInEveryVMRepeatingIfNecessary(runnable, 0);
  }

  /**
   * Invokes a <code>SerializableRunnable</code> in every VM that
   * DUnit knows about.  If <code>run()</code> throws an assertion failure, 
   * its execution is repeated, until no assertion failure occurs or
   * <code>repeatTimeoutMs</code> milliseconds have passed.
   * 
   * @see VM#invoke(RepeatableRunnable)
   * @deprecated Please use {@link com.jayway.awaitility.Awaitility} with {@link #invokeInEveryVM(SerializableCallableIF)} instead.
   */
  public static void invokeInEveryVMRepeatingIfNecessary(final RepeatableRunnable runnable, final long repeatTimeoutMs) {
    for (int h = 0; h < Host.getHostCount(); h++) {
      Host host = Host.getHost(h);
  
      for (int v = 0; v < host.getVMCount(); v++) {
        VM vm = host.getVM(v);
        vm.invokeRepeatingIfNecessary(runnable, repeatTimeoutMs);
      }
    }
  }
}
