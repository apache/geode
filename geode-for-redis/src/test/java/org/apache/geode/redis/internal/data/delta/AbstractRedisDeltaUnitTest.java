package org.apache.geode.redis.internal.data.delta;

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.redis.internal.data.RedisHash;
import org.apache.geode.redis.internal.data.RedisList;
import org.apache.geode.redis.internal.data.RedisSet;
import org.apache.geode.redis.internal.data.RedisSortedSet;

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
    RedisSet redisSet = new RedisSet(5);
    redisSet.membersAdd("zero".getBytes());
    redisSet.membersAdd("one".getBytes());
    redisSet.membersAdd("two".getBytes());
    return redisSet;
  }

  protected RedisSortedSet makeRedisSortedSet() {
    RedisSortedSet redisSortedSet = new RedisSortedSet(3);
    redisSortedSet.memberAdd("alpha".getBytes(), 1.0d);
    redisSortedSet.memberAdd("beta".getBytes(), 2.0d);
    redisSortedSet.memberAdd("gamma".getBytes(), 4.0d);
    return redisSortedSet;
  }
}
