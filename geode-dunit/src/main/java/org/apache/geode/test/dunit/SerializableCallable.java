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

import static java.lang.Integer.toHexString;

import java.io.Serializable;
import java.util.concurrent.Callable;

/**
 * This interface provides both {@link Serializable} and {@link Callable}. It is often used in
 * conjunction with {@link VM#invoke(SerializableCallableIF)}.
 *
 * <pre>
 * public void testReplicatedRegionPut() {
 *   VM vm0 = VM.getVM(0);
 *   VM vm1 = VM.getVM(1);
 *
 *   String name = getUniqueName();
 *   Object value = new Integer(42);
 *
 *   SerializableCallable putMethod = new SerializableCallable("Replicated put") {
 *       public Object call() throws Exception {
 *         ...// get replicated test region //...
 *         return region.put(name, value);
 *       }
 *      });
 *   assertNull(vm0.invoke(putMethod));
 *   assertIndexDetailsEquals(value, vm1.invoke(putMethod));
 *  }
 * </pre>
 */
public abstract class SerializableCallable<T> implements SerializableCallableIF<T> {

  private static final String DEFAULT_NAME = "";
  private static final long DEFAULT_ID = 0L;

  private final String name;
  private final long id;

  public SerializableCallable() {
    this(DEFAULT_NAME, DEFAULT_ID);
  }

  /**
   * @deprecated Please pass name as the first argument to {@link VM} invoke or asyncInvoke.
   */
  @Deprecated
  public SerializableCallable(String name) {
    this(name, DEFAULT_ID);
  }

  public SerializableCallable(long id) {
    this(DEFAULT_NAME, id);
  }

  private SerializableCallable(String name, long id) {
    this.name = name;
    this.id = id;
  }

  /**
   * @deprecated Please pass name as the first argument to {@link VM} invoke or asyncInvoke.
   */
  @Deprecated
  public String getName() {
    return name;
  }

  public long getId() {
    return id;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "@" + toHexString(hashCode())
        + '(' + id + ", \"" + name + "\")";
  }
}
