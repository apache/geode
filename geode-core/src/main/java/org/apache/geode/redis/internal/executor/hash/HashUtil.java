package org.apache.geode.redis.internal.executor.hash;

import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;

/**
 * 
 * @author Gregory Green
 *
 */
public class HashUtil {
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
    int nameSeparatorIndex = keyString.indexOf(Coder.REGION_KEY_SEPERATOR);
    if (nameSeparatorIndex > -1) {
      keyString = keyString.substring(0, nameSeparatorIndex);
      key = new ByteArrayWrapper(Coder.stringToBytes(keyString));
    }

    return key;
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
    int nameSeparatorIndex = keyString.indexOf(Coder.REGION_KEY_SEPERATOR);
    if (nameSeparatorIndex < 0) {
      return key;
    }

    keyString = keyString.substring(nameSeparatorIndex + 1);
    key = new ByteArrayWrapper(Coder.stringToBytes(keyString));
    return key;
  }
}
