package org.apache.geode.redis.internal.executor.hash;

import java.util.Map;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.ExecutionHandlerContext;

/**
 * 
 * @author Gregory Green
 *
 */
public class HashInterpreter {

  /**
   * REGION_KEY_SEPERATOR = ":"
   */
  public static final String REGION_KEY_SEPERATOR = ":";

  /**
   * The default hash region name REGION_HASH_REGION = "RedisHash"
   */
  public static final ByteArrayWrapper REGION_HASH_REGION =
      Coder.stringToByteArrayWrapper("RedisHash");

  @SuppressWarnings("unchecked")
  public static Region<ByteArrayWrapper, Map<ByteArrayWrapper, ByteArrayWrapper>> getRegion(
      ByteArrayWrapper key, ExecutionHandlerContext context) {
    return (Region<ByteArrayWrapper, Map<ByteArrayWrapper, ByteArrayWrapper>>) context
        .getRegionProvider().getRegion(toRegionNameByteArray(key));
  }// ------------------------------------------------

  /**
   * Supports conversation of keys with format region:key
   * 
   * @param key the byte array wrapper
   * @return the byte array wrapper
   */
  public static ByteArrayWrapper toRegionNameByteArray(ByteArrayWrapper key) {
    if (key == null)
      return null;

    String keyString = key.toString();
    int nameSeparatorIndex = keyString.indexOf(REGION_KEY_SEPERATOR);
    if (nameSeparatorIndex > -1) {
      keyString = keyString.substring(0, nameSeparatorIndex);
      key = new ByteArrayWrapper(Coder.stringToBytes(keyString));
    }

    return REGION_HASH_REGION;
  }

  /**
   * Return key from format region:key
   * 
   * @param key the raw key
   * @return the ByteArray for the key
   */
  public static ByteArrayWrapper toEntryKey(ByteArrayWrapper key) {
    if (key == null)
      return null;

    String keyString = key.toString();
    int nameSeparatorIndex = keyString.indexOf(REGION_KEY_SEPERATOR);
    if (nameSeparatorIndex < 0) {
      return key;
    }

    keyString = keyString.substring(nameSeparatorIndex + 1);
    key = new ByteArrayWrapper(Coder.stringToBytes(keyString));
    return key;
  }
}
