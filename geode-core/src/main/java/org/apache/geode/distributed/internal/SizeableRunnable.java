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
package org.apache.geode.distributed.internal;


/**
 * SizeableRunnable is a Runnable with a size. It implements the Sizeable interface for use in
 * QueuedExecutors using a ThrottlingMemLinkedBlockingQueue.
 * <p>
 *
 * Instances/subclasses must provide the run() method to complete the Runnable interface.
 * <p>
 *
 * @since GemFire 5.0
 */

public abstract class SizeableRunnable implements Runnable, Sizeable {
  private final int size;

  public SizeableRunnable(int size) {
    this.size = size;
  }

  @Override
  public int getSize() {
    return size;
  }
}
