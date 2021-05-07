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
 *
 */

package org.apache.geode.redis.internal.netty;

import java.util.Objects;

import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;


public class Client {
  private Channel channel;

  public Client(Channel remoteAddress) {
    this.channel = remoteAddress;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Client client = (Client) o;
    return Objects.equals(channel, client.channel);
  }

  @Override
  public int hashCode() {
    return Objects.hash(channel);
  }

  public void addShutdownListener(
      GenericFutureListener<? extends Future<? super Void>> shutdownListener) {
    channel.closeFuture().addListener(shutdownListener);
  }

  public boolean isDead() {
    return !this.channel.isOpen();
  }

  public String toString() {
    return channel.toString();
  }
}
