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
package org.apache.geode.internal.size;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.internal.util.concurrent.CopyOnWriteWeakHashMap;

/**
 * An implementation of {@link ObjectSizer} that calculates an accurate, in memory size of for the
 * first instance of each class that it sees. After the first size calculation, it will return the
 * same size for every instance of that class.
 *
 * This sizer is a compromise between generating accurate sizes for every object and performance. It
 * should work well for objects that are fairly constant in size. For completely accurate sizing,
 * use {@link ReflectionObjectSizer}
 *
 *
 */
public class SizeClassOnceObjectSizer implements ObjectSizer, Serializable, Declarable {

  @Immutable
  private static final SizeClassOnceObjectSizer INSTANCE = new SizeClassOnceObjectSizer();

  private final transient Map<Class, Integer> savedSizes =
      new CopyOnWriteWeakHashMap<>();

  private final transient ReflectionObjectSizer sizer = ReflectionObjectSizer.getInstance();

  @Override
  public int sizeof(Object o) {
    if (o == null) {
      return 0;
    }
    int wellKnownObjectSize = WellKnownClassSizer.sizeof(o);
    if (wellKnownObjectSize != 0) {
      return wellKnownObjectSize;
    }

    // Now do the sizing
    Class clazz = o.getClass();
    Integer size = savedSizes.get(clazz);
    if (size == null) {
      size = Integer.valueOf(sizer.sizeof(o));
      savedSizes.put(clazz, size);
    }
    return size.intValue();
  }

  public static SizeClassOnceObjectSizer getInstance() {
    return INSTANCE;
  }

  // This object is serializable because EvictionAttributes is serializable
  // We want to resolve to the same singleton when deserializing
  private void writeObject(java.io.ObjectOutputStream out) throws IOException {

  }

  private void readObject(java.io.ObjectInputStream in)
      throws IOException, ClassNotFoundException {}

  private Object readResolve() throws ObjectStreamException {
    return INSTANCE;
  }

  private SizeClassOnceObjectSizer() {

  }

  @Override
  public void init(Properties props) {
    // TODO Auto-generated method stub

  }
}
