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
package org.apache.geode.internal.cache.wan.asyncqueue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.distributed.DistributedMember;

/**
 * Created by dan on 5/10/17.
 */
public abstract class AbstractMovingAsyncEventListener implements AsyncEventListener {
  protected final DistributedMember destination;
  boolean moved;
  Set<Object> keysSeen = new HashSet<Object>();

  public AbstractMovingAsyncEventListener(final DistributedMember destination) {
    this.destination = destination;
  }

  @Override
  public boolean processEvents(final List<AsyncEvent> events) {
    if (!moved) {

      AsyncEvent event1 = events.get(0);
      move(event1);
      moved = true;
      return false;
    }

    events.stream().map(AsyncEvent::getKey).forEach(keysSeen::add);
    return true;
  }

  protected abstract void move(AsyncEvent event1);

  @Override
  public void close() {

  }
}
