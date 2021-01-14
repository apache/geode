/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.test.dunit.rules;

import static org.apache.geode.test.dunit.VM.DEFAULT_VM_COUNT;
import static org.apache.geode.util.internal.CompletionUtils.close;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.geode.test.dunit.VM;

/**
 * DistributedReference is a JUnit Rule that provides automated tearDown for a static
 * reference in every distributed test {@code VM}s including the main JUnit controller {@code VM}.
 * If the referenced value is an {@code AutoCloseable} or {@code Closeable} then it will be
 * auto-closed and set to null in every {@code VM} during tear down.
 *
 * <p>
 * If the referenced value is not an {@code AutoCloseable} or {@code Closeable}, the
 * {@code DistributedReference} will use reflection to invoke any method named
 * {@code close}, {@code disconnect}, or {@code stop} regardless of what interfaces are implemented
 * unless {@code autoClose} is set to false.
 *
 * <p>
 * If the referenced value is null in any {@code VM} then it will be ignored in that {@code VM}
 * during tear down.
 *
 * <p>
 * In the following example, every {@code VM} has a {@code ServerLauncher} which will be
 * auto-stopped and set to null during tear down:
 *
 * <pre>
 * {@literal @}Rule
 * public DistributedReference&lt;ServerLauncher&gt; server = new DistributedReference&lt;&gt;();
 *
 * {@literal @}Before
 * public void setUp() throws IOException {
 *   Properties configProperties = new Properties();
 *   configProperties.setProperty(LOCATORS, DistributedRule.getLocators());
 *
 *   for (VM vm : toArray(getVM(0), getVM(1), getVM(2), getVM(3), getController())) {
 *     vm.invoke(() -> {
 *       server.set(new ServerLauncher.Builder()
 *           .setMemberName("server" + getVMId())
 *           .setDisableDefaultServer(true)
 *           .reinitializeWithWorkingDirectory(temporaryFolder.newFolder("server" + getVMId()).getAbsolutePath())
 *           .build());
 *
 *       server.get().start();
 *     });
 *   }
 * }
 *
 * {@literal @}Test
 * public void eachVmHasItsOwnServerCache() {
 *   for (VM vm : toArray(getVM(0), getVM(1), getVM(2), getVM(3), getController())) {
 *     vm.invoke(() -> {
 *       assertThat(server.get().getCache()).isNotNull();
 *     });
 * }
 * </pre>
 *
 * <p>
 * In the following example, every {@code VM} has a {@code Cache} which will be auto-closed and set
 * to null during tear down:
 *
 * <pre>
 * {@literal @}Rule
 * public DistributedReference&lt;Cache&gt; cache = new DistributedReference&lt;&gt;();
 *
 * {@literal @}Before
 * public void setUp() {
 *   Properties configProperties = new Properties();
 *   configProperties.setProperty(LOCATORS, DistributedRule.getLocators());
 *
 *   for (VM vm : toArray(getVM(0), getVM(1), getVM(2), getVM(3), getController())) {
 *     vm.invoke(() -> {
 *       cache.set(new CacheFactory(configProperties).create());
 *     });
 *   }
 * }
 *
 * {@literal @}Test
 * public void eachVmHasItsOwnCache() {
 *   for (VM vm : toArray(getVM(0), getVM(1), getVM(2), getVM(3), getController())) {
 *     vm.invoke(() -> {
 *       assertThat(cache.get()).isNotNull();
 *     });
 *   }
 * }
 * </pre>
 *
 * <p>
 * In the following example, every {@code VM} has a {@code DistributedSystem} which will be
 * auto-disconnected and set to null during tear down:
 *
 * <pre>
 * {@literal @}Rule
 * public DistributedReference&lt;DistributedSystem&gt; system = new DistributedReference&lt;&gt;();
 *
 * {@literal @}Before
 * public void setUp() {
 *   Properties configProperties = new Properties();
 *   configProperties.setProperty(LOCATORS, DistributedRule.getLocators());
 *
 *   for (VM vm : toArray(getVM(0), getVM(1), getVM(2), getVM(3), getController())) {
 *     vm.invoke(() -> {
 *       system.set(DistributedSystem.connect(configProperties));
 *     });
 *   }
 * }
 *
 * {@literal @}Test
 * public void eachVmHasItsOwnDistributedSystemConnection() {
 *   for (VM vm : toArray(getVM(0), getVM(1), getVM(2), getVM(3), getController())) {
 *     vm.invoke(() -> {
 *       assertThat(system.get()).isNotNull();
 *     });
 *   }
 * }
 * </pre>
 *
 * <p>
 * To disable auto-closing in a test, specify {@code autoClose(false)}:
 *
 * <pre>
 * {@literal @}Rule
 * public DistributedReference&lt;ServerLauncher&gt; serverLauncher =
 *     new DistributedReference&lt;&gt;().autoClose(false);
 * </pre>
 *
 * <p>
 * The {@code DistributedReference} value will still be set to null during tear down even
 * if auto-closing is disabled.
 */
@SuppressWarnings({"serial", "unused", "WeakerAccess",
    "OverloadedMethodsWithSameNumberOfParameters"})
public class DistributedReference<V> extends AbstractDistributedRule {

  private static final AtomicReference<Map<Integer, Object>> REFERENCE = new AtomicReference<>();

  private final AtomicBoolean autoClose = new AtomicBoolean(true);
  private final int identity;

  public DistributedReference() {
    this(DEFAULT_VM_COUNT);
  }

  public DistributedReference(int vmCount) {
    super(vmCount);
    identity = hashCode();
  }

  /**
   * Set false to disable autoClose during tearDown. Default is true.
   */
  public DistributedReference<V> autoClose(boolean value) {
    autoClose.set(value);
    return this;
  }

  /**
   * Gets the current value.
   *
   * @return the current value
   */
  public V get() {
    return uncheckedCast(REFERENCE.get().get(identity));
  }

  /**
   * Sets to the given value.
   *
   * @param newValue the new value
   */
  public DistributedReference<V> set(V newValue) {
    REFERENCE.get().put(identity, newValue);
    return this;
  }

  @Override
  protected void before() {
    invoker().invokeInEveryVMAndController(() -> invokeBefore());
  }

  @Override
  protected void after() {
    invoker().invokeInEveryVMAndController(() -> invokeAfter());
  }

  @Override
  protected void afterCreateVM(VM vm) {
    vm.invoke(() -> invokeBefore());
  }

  @Override
  @SuppressWarnings("RedundantMethodOverride")
  protected void beforeBounceVM(VM vm) {
    // override if needed
  }

  @Override
  protected void afterBounceVM(VM vm) {
    vm.invoke(() -> invokeBefore());
  }

  private void invokeBefore() {
    REFERENCE.compareAndSet(null, new HashMap<>());
    REFERENCE.get().putIfAbsent(identity, null);
  }

  private void invokeAfter() {
    Map<Integer, Object> references = REFERENCE.getAndSet(null);
    if (references == null) {
      return;
    }

    for (Object object : references.values()) {
      invokeAfter(object);
    }
  }

  private void invokeAfter(Object object) {
    if (object == null) {
      return;
    }

    if (autoClose.get()) {
      autoClose(object);
    }
  }

  private void autoClose(Object object) {
    if (object instanceof AutoCloseable) {
      close((AutoCloseable) object);

    } else if (object instanceof AtomicBoolean) {
      close((AtomicBoolean) object);

    } else if (object instanceof CountDownLatch) {
      close((CountDownLatch) object);

    } else if (hasMethod(object.getClass(), "close")) {
      invokeMethod(object, "close");

    } else if (hasMethod(object.getClass(), "disconnect")) {
      invokeMethod(object, "disconnect");

    } else if (hasMethod(object.getClass(), "stop")) {
      invokeMethod(object, "stop");
    }
  }

  private static boolean hasMethod(Class<?> objectClass, String methodName) {
    try {
      Method method = objectClass.getMethod(methodName);
      Class<?> returnType = method.getReturnType();
      // currently only supports public method with zero parameters
      if (method.getParameterCount() == 0 &&
          Modifier.isPublic(method.getModifiers())) {
        return true;
      }
    } catch (NoSuchMethodException e) {
      // ignore
    }
    return false;
  }

  private static void invokeMethod(Object object, String methodName) {
    try {
      Method method = object.getClass().getMethod(methodName);
      method.invoke(object);
    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
