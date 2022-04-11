package org.apache.geode.redis.internal.data.delta;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.geode.redis.internal.data.RedisHash;
import org.apache.geode.redis.internal.data.RedisList;
import org.apache.geode.redis.internal.data.RedisSet;
import org.apache.geode.redis.internal.data.RedisSortedSet;
import org.apache.geode.redis.internal.data.RedisString;

public abstract class AbstractRedisDeltaUnitTest {
  protected RedisHash makeRedisHash() {
    List<byte[]> pairList = new ArrayList<>();
    pairList.add("zero".getBytes());
    pairList.add("firstVal".getBytes());
    pairList.add("one".getBytes());
    pairList.add("secondVal".getBytes());
    pairList.add("two".getBytes());
    pairList.add("thirdVal".getBytes());
    return new RedisHash(pairList);
  }

  protected RedisList makeRedisList() {
    RedisList redisList = new RedisList();
    redisList.applyAddByteArrayTailDelta("zero".getBytes());
    redisList.applyAddByteArrayTailDelta("one".getBytes());
    redisList.applyAddByteArrayTailDelta("two".getBytes());
    return redisList;
  }

  protected RedisSet makeRedisSet() {
    Set<byte[]> bytes = new HashSet<>();
    bytes.add("zero".getBytes());
    bytes.add("one".getBytes());
    bytes.add("two".getBytes());
    return new RedisSet(bytes);
  }

  protected RedisSortedSet makeRedisSortedSet() {
    List<byte[]> members = Arrays.asList("alpha".getBytes(), "beta".getBytes(), "gamma".getBytes());
    double[] scores = {1.0d, 2.0d, 4.0d};
    return new RedisSortedSet(members, scores);
  }

  protected RedisString makeRedisString() {
    return new RedisString("radish".getBytes());
  }
}
