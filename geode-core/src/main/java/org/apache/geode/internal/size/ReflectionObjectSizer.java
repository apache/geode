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

import org.apache.logging.log4j.Logger;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.PlaceHolderDiskRegion;
import org.apache.geode.internal.size.ObjectGraphSizer.ObjectFilter;
import org.apache.geode.internal.statistics.StatisticsManager;

/**
 * An implementation of {@link ObjectSizer} that calculates an accurate, in memory size of for each
 * object that it sizes. This is the slowest method of calculating sizes, but it should accurately
 * reflect the amount of heap memory used for objects.
 *
 * This class will traverse all objects that are reachable from the passed in object by instance
 * fields. So use this class with caution if you have instance fields that refer to shared objects.
 *
 * For objects that are all approximately the same size, consider using
 * {@link SizeClassOnceObjectSizer}
 *
 *
 */
public class ReflectionObjectSizer implements ObjectSizer, Serializable {

  @Immutable
  private static final ReflectionObjectSizer INSTANCE = new ReflectionObjectSizer();

  @Immutable
  private static final ObjectFilter FILTER = new ObjectFilter() {

    @Override
    public boolean accept(Object parent, Object object) {
      // Protect the user from a couple of pitfalls. If their object
      // has a link to a region or cache, we don't want to size the whole thing.
      return !(object instanceof Region)
          && !(object instanceof Cache)
          && !(object instanceof PlaceHolderDiskRegion)
          && !(object instanceof InternalDistributedSystem)
          && !(object instanceof ClassLoader)
          && !(object instanceof Logger)
          && !(object instanceof StatisticsManager);
    }

  };

  @Override
  public int sizeof(Object o) {
    try {
      return (int) ObjectGraphSizer.size(o, FILTER, false);
    } catch (IllegalArgumentException e) {
      throw new InternalGemFireError(e);
    } catch (IllegalAccessException e) {
      throw new InternalGemFireError(e);
    }
  }

  public static ReflectionObjectSizer getInstance() {
    return INSTANCE;
  }

  private void writeObject(java.io.ObjectOutputStream out) throws IOException {}

  private void readObject(java.io.ObjectInputStream in)
      throws IOException, ClassNotFoundException {}

  private Object readResolve() throws ObjectStreamException {
    return INSTANCE;
  }

  private ReflectionObjectSizer() {

  }

}
