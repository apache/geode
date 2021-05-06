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

package org.apache.geode.redis.internal.proxy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.netty.channel.Channel;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.apache.commons.lang3.tuple.Pair;

public class ClusterSlotsResponseProcessor implements RedisResponseProcessor {

  private final Map<HostPort, HostPort> mappings;

  public ClusterSlotsResponseProcessor(Map<HostPort, HostPort> mappings) {
    this.mappings = mappings;
  }

  /**
   * CLUSTER SLOTS looks something like this:
   *
   * <pre>
   * 1) 1) (integer) 0
   *    2) (integer) 5460
   *    3) 1) "127.0.0.1"
   *       2) (integer) 30001
   *       3) "09dbe9720cda62f7865eabc5fd8857c5d2678366"
   *    4) 1) "127.0.0.1"
   *       2) (integer) 30004
   *       3) "821d8ca00d7ccf931ed3ffc7e3db0599d2271abf"
   * 2) 1) (integer) 5461
   *    2) (integer) 10922
   *    3) 1) "127.0.0.1"
   *       2) (integer) 30002
   *       3) "c9d93d9f2c0c524ff34cc11838c2003d8c29e013"
   *    4) 1) "127.0.0.1"
   *       2) (integer) 30005
   *       3) "faadb3eb99009de4ab72ad6b6ed87634c7ee410f"
   * </pre>
   */
  @Override
  public Object process(Object message, Channel channel) {
    if (message instanceof ErrorRedisMessage) {
      return message;
    }

    ArrayRedisMessage input = (ArrayRedisMessage) message;
    List<RedisMessage> response = new ArrayList<>();

    for (RedisMessage entry : input.children()) {
      List<RedisMessage> newInner = new ArrayList<>();
      ArrayRedisMessage inner = (ArrayRedisMessage) entry;

      // slot start
      newInner.add(inner.children().get(0));
      // slot end
      newInner.add(inner.children().get(1));

      for (int i = 2; i < inner.children().size(); i++) {
        ArrayRedisMessage hostPortArray = (ArrayRedisMessage) inner.children().get(i);
        RedisMessage hostMessagePart = hostPortArray.children().get(0);
        String host = ((FullBulkStringRedisMessage) hostMessagePart)
            .content().toString(CharsetUtil.UTF_8);

        RedisMessage portMessagePart = hostPortArray.children().get(1);
        Integer port = (int) ((IntegerRedisMessage) portMessagePart).value();

        Pair<String, Integer> newMapping = getMapping(host, port);

        List<RedisMessage> newHostPortArray = new ArrayList<>();
        newHostPortArray.add(new SimpleStringRedisMessage(newMapping.getLeft()));
        newHostPortArray.add(new IntegerRedisMessage(newMapping.getRight()));
        for (int j = 2; j < hostPortArray.children().size(); j++) {
          newHostPortArray.add(hostPortArray.children().get(j));
        }

        newInner.add(new ArrayRedisMessage(newHostPortArray));

        // Since we've replaced these, we need to free up the originals
        ReferenceCountUtil.release(hostMessagePart);
        ReferenceCountUtil.release(portMessagePart);
      }

      response.add(new ArrayRedisMessage(newInner));
    }

    return new ArrayRedisMessage(response);
  }

  private Pair<String, Integer> getMapping(String host, Integer port) {
    for (Map.Entry<HostPort, HostPort> entry : mappings.entrySet()) {
      HostPort from = entry.getKey();
      if (from.getHost().equals(host) && from.getPort().equals(port)) {
        return Pair.of(entry.getValue().getHost(), entry.getValue().getPort());
      }
    }

    throw new IllegalArgumentException("Unable to map host and port " + host + ":" + port);
  }
}
