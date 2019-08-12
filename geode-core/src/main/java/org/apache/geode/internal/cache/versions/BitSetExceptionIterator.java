package org.apache.geode.internal.cache.versions;

import java.util.BitSet;
import java.util.Iterator;

public class BitSetExceptionIterator implements Iterator<RVVException> {
  private final BitSet bitSet;
  private long bitSetVersion;
  private final long newVersion;
  private long nextClearBit;

  public BitSetExceptionIterator(BitSet bitSet, long bitSetVersion, long newVersion) {
    this.bitSet = bitSet;
    this.bitSetVersion = bitSetVersion;
    this.newVersion = newVersion;
    this.nextClearBit = findNextClearBit(bitSet, 0);
  }

  private int findNextClearBit(BitSet bitSet, int fromIndex) {
    int nextClearBit = bitSet.nextClearBit(fromIndex);

    long lastSetBit = newVersion - bitSetVersion;
    if (nextClearBit >= lastSetBit) {
      // We found empty bits, but past the offset we are interested in
      // Ignore these
      return -1;
    }

    return nextClearBit;
  }

  @Override
  public boolean hasNext() {
    return nextClearBit != -1;
  }

  @Override
  public RVVException next() {
    if (!hasNext()) {
      return null;
    }

    int nextSetBit = bitSet.nextSetBit((int) Math.min(Integer.MAX_VALUE, nextClearBit));
    long nextSetVersion = nextSetBit == -1 ? newVersion : nextSetBit + bitSetVersion;

    RVVException exception =
        RVVException.createException(nextClearBit + bitSetVersion - 1, nextSetVersion);

    nextClearBit = nextSetBit == -1 ? -1 : findNextClearBit(bitSet, nextSetBit);

    return exception;
  }
}
