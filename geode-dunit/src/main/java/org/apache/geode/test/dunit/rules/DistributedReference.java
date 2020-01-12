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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * JUnit Rule that provides a static reference in every distributed test {@code VM}s including the
 * main JUnit controller {@code VM}. If the referenced value is an {@code AutoCloseable} or
 * {@code Closeable} then it will be auto-closed in every {@code VM} during tear down.
 *
 * <p>
 * If the referenced value is not an {@code AutoCloseable} or {@code Closeable}, the
 * {@code DistributedReference} will use reflection to invoke any method named {@code close} or
 * {@code disconnect} regardless of what interfaces are implemented.
 *
 * <p>
 * If the referenced value is null in any {@code VM} then it will be ignored in that {@code VM}
 * during tear down.
 *
 * <p>
 * In the following example, every {@code VM} has a {@code Cache} which will be auto-closed during
 * tear down:
 *
 * <pre>
 * {@literal @}Rule
 * public DistributedReference&lt;Cache&gt; cache = new DistributedReference&lt;&gt;();
 *
 * {@literal @}Before
 * public void setUp() {
 *   Properties configProperties = new Properties();
 *   configProperties.setProperty(LOCATORS, DistributedTestUtils.getLocators());
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
 *       assertThat(cache.get()).isInstanceOf(Cache.class);
 *     });
 *   }
 * }
 * </pre>
 *
 * <p>
 * In the following example, every {@code VM} has a {@code DistributedSystem} which will be
 * auto-disconnected during tear down:
 *
 * <pre>
 * {@literal @}Rule
 * public DistributedReference&lt;DistributedSystem&gt; system = new DistributedReference&lt;&gt;();
 *
 * {@literal @}Before
 * public void setUp() {
 *   Properties configProperties = new Properties();
 *   configProperties.setProperty(LOCATORS, DistributedTestUtils.getLocators());
 *
 *   for (VM vm : toArray(getVM(0), getVM(1), getVM(2), getVM(3), getController())) {
 *     vm.invoke(() -> {
 *       system.set(DistributedSystem.connect(configProperties));
 *     });
 *   }
 * }
 *
 * {@literal @}Test
 * public void eachVmHasItsOwnSystemConnection() {
 *   for (VM vm : toArray(getVM(0), getVM(1), getVM(2), getVM(3), getController())) {
 *     vm.invoke(() -> {
 *       assertThat(system.get()).isInstanceOf(DistributedSystem.class);
 *     });
 *   }
 * }
 * </pre>
 *
 * <p>
 * To disable autoClose in a test, specify {@code autoClose(false)}:
 *
 * <pre>
 * {@literal @}Rule
 * public DistributedReference&lt;DistributedSystem&gt; system =
 *     new DistributedReference&lt;&gt;().autoClose(false);
 * </pre>
 */
@SuppressWarnings({"serial", "unused"})
public class DistributedReference<V> extends AbstractDistributedRule {

  private static final AtomicReference<Object> reference = new AtomicReference<>();

  private final AtomicBoolean autoClose = new AtomicBoolean(true);

  public DistributedReference() {
    this(DEFAULT_VM_COUNT);
  }

  public DistributedReference(int vmCount) {
    super(vmCount);
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
  @SuppressWarnings("unchecked")
  public V get() {
    return (V) reference.get();
  }

  /**
   * Sets to the given value.
   *
   * @param newValue the new value
   */
  public void set(V newValue) {
    reference.set(newValue);
  }

  @Override
  protected void after() {
    invoker().invokeInEveryVMAndController(this::invokeAfter);
  }

  private void invokeAfter() {
    V value = get();
    if (value == null) {
      return;
    }
    reference.set(null);

    if (autoClose.get()) {
      autoClose(value);
    }
  }

  private void autoClose(V value) {
    if (value instanceof AutoCloseable) {
      close((AutoCloseable) value);

    } else if (hasMethod(value.getClass(), "close")) {
      invokeMethod(value, "close");

    } else if (hasMethod(value.getClass(), "disconnect")) {
      invokeMethod(value, "disconnect");

    } else if (hasMethod(value.getClass(), "stop")) {
      invokeMethod(value, "stop");
    }
  }

  private static void close(AutoCloseable autoCloseable) {
    try {
      autoCloseable.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
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
