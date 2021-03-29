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
package org.apache.geode.test.junit.rules;

import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.geode.test.junit.rules.serializable.SerializableExternalResource;

/**
 * CloseableReference is a JUnit Rule that provides automated tearDown for an atomic reference. If
 * the referenced value is an {@code AutoCloseable} or {@code Closeable} then it will be auto-closed
 * and set to null during tear down.
 *
 * <p>
 * If the referenced value is not an {@code AutoCloseable} or {@code Closeable}, the
 * {@code CloseableReference} will use reflection to invoke any method named {@code close},
 * {@code disconnect}, or {@code stop} regardless of what interfaces are implemented unless
 * {@code autoClose} is set to false.
 *
 * <p>
 * If the referenced value is null then it will be ignored during tear down.
 *
 * <p>
 * In the following example, the test has a {@code ServerLauncher} which will be
 * auto-stopped and set to null during tear down:
 *
 * <pre>
 * {@literal @}Rule
 * public CloseableReference&lt;ServerLauncher&gt; server = new CloseableReference&lt;&gt;();
 *
 * {@literal @}Before
 * public void setUp() throws IOException {
 *   server.set(new ServerLauncher.Builder()
 *       .setMemberName("server1")
 *       .setDisableDefaultServer(true)
 *       .reinitializeWithWorkingDirectory(temporaryFolder.newFolder("server1").getAbsolutePath())
 *       .build());
 *
 *   server.get().start();
 * }
 *
 * {@literal @}Test
 * public void serverHasCache() {
 *   assertThat(server.get().getCache()).isNotNull();
 * }
 * </pre>
 *
 * <p>
 * In the following example, the test has a {@code Cache} which will be auto-closed and set to null
 * during tear down:
 *
 * <pre>
 * {@literal @}Rule
 * public CloseableReference&lt;Cache&gt; cache = new CloseableReference&lt;&gt;();
 *
 * {@literal @}Before
 * public void setUp() {
 *   cache.set(new CacheFactory().create());
 * }
 *
 * {@literal @}Test
 * public void cacheExists() {
 *   assertThat(cache.get()).isNotNull();
 * }
 * </pre>
 *
 * <p>
 * In the following example, the test has a {@code DistributedSystem} which will be
 * auto-disconnected and set to null during tear down:
 *
 * <pre>
 * {@literal @}Rule
 * public CloseableReference&lt;DistributedSystem&gt; system = new CloseableReference&lt;&gt;();
 *
 * {@literal @}Before
 * public void setUp() {
 *   system.set(DistributedSystem.connect());
 * }
 *
 * {@literal @}Test
 * public void distributedSystemExists() {
 *   assertThat(system.get()).isNotNull();
 * }
 * </pre>
 *
 * <p>
 * To disable auto-closing in a test, specify {@code autoClose(false)}:
 *
 * <pre>
 * {@literal @}Rule
 * public CloseableReference&lt;ServerLauncher&gt; serverLauncher =
 *     new CloseableReference&lt;&gt;().autoClose(false);
 * </pre>
 *
 * <p>
 * The {@code CloseableReference} value will still be set to null during tear down even if
 * auto-closing is disabled.
 */
@SuppressWarnings({"serial", "WeakerAccess"})
public class CloseableReference<V> extends SerializableExternalResource {

  private static final AtomicReference<Object> reference = new AtomicReference<>();

  private final AtomicBoolean autoClose = new AtomicBoolean(true);

  /**
   * Set false to disable autoClose during tearDown. Default is true.
   */
  public CloseableReference<V> autoClose(boolean value) {
    autoClose.set(value);
    return this;
  }

  /**
   * Gets the current value.
   *
   * @return the current value
   */
  public V get() {
    return uncheckedCast(reference.get());
  }

  /**
   * Sets to the given value.
   *
   * @param newValue the new value
   */
  public CloseableReference<V> set(V newValue) {
    reference.set(newValue);
    return this;
  }

  @Override
  protected void after() {
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
