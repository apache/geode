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
package org.apache.geode.internal.concurrent;

/**
 * AL implementation for JDK 5.
 */
public class AtomicLong5 extends java.util.concurrent.atomic.AtomicLong implements AL {
  private static final long serialVersionUID = -1915700199064062938L;

  public AtomicLong5() {
    super();
  }

  public AtomicLong5(long initialValue) {
    super(initialValue);
  }

  /**
   * Use it only when threads are doing incremental updates. If updates are random then this method
   * may not be optimal.
   */
  @Override
  public boolean setIfGreater(long update) {
    while (true) {
      long cur = get();

      if (update > cur) {
        if (compareAndSet(cur, update)) {
          return true;
        }
      } else {
        return false;
      }
    }
  }
}
