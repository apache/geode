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
package org.apache.geode.test.dunit.internal;

import org.apache.geode.test.dunit.SerializableRunnableIF;

public class IdentifiableRunnable implements SerializableRunnableIF {

  private final long id;
  private final String name;
  private final SerializableRunnableIF delegate;

  public IdentifiableRunnable(String name, SerializableRunnableIF delegate) {
    this(0, name, delegate);
  }

  public IdentifiableRunnable(long id, SerializableRunnableIF delegate) {
    this(id, String.valueOf(id), delegate);
  }

  public IdentifiableRunnable(long id, String name, SerializableRunnableIF delegate) {
    this.id = id;
    this.name = name;
    this.delegate = delegate;
  }

  @Override
  public void run() throws Exception {
    delegate.run();
  }

  @Override
  public long getId() {
    return id;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + "(" + id + ":" + name + ")";
  }
}
