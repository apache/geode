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
package com.gemstone.gemfire.internal.cache;

/**
 * Represents a factory for instances of {@link ReliableMessageQueue}.
 * The Cache will have an instance of the factory that can be obtained
 * from {@link GemFireCacheImpl#getReliableMessageQueueFactory}.
 * 
 * @since GemFire 5.0
 */
public interface ReliableMessageQueueFactory {
  /**
   * Creates an instance of {@link ReliableMessageQueue} given the region
   * that the queue will be on.
   * @param region the distributed region that the created queue will service.
   * @return the created queue
   */
  public ReliableMessageQueue create(DistributedRegion region);
  /**
   * Cleanly shutdown this factory flushing any persistent data to disk.
   * @param force true if close should always work
   * @throws IllegalStateException if <code>force</code> is false and the factory
   * is still in use. The factory is in use as long as a queue it produced remains
   * unclosed.
   */
  public void close(boolean force);
}
