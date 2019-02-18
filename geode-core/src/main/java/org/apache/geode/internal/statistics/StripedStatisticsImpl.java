package org.apache.geode.internal.statistics;

import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

import org.apache.geode.StatisticsType;

/**
 * Stripes statistic counters across threads to reduce contention using {@link LongAdder} and
 * {@link DoubleAdder}.
 */
public class StripedStatisticsImpl extends StatisticsImpl {

  private final LongAdder[] intAdders;
  private final LongAdder[] longAdders;
  private final DoubleAdder[] doubleAdders;

  public StripedStatisticsImpl(StatisticsType type, String textId, long numericId,
      long uniqueId) {
    super(type, textId, numericId, uniqueId, 0);

    StatisticsTypeImpl realType = (StatisticsTypeImpl) type;

    this.intAdders =
        Stream.generate(LongAdder::new).limit(realType.getIntStatCount()).toArray(LongAdder[]::new);
    this.longAdders =
        Stream.generate(LongAdder::new).limit(realType.getLongStatCount())
            .toArray(LongAdder[]::new);
    this.doubleAdders =
        Stream.generate(DoubleAdder::new).limit(realType.getDoubleStatCount())
            .toArray(DoubleAdder[]::new);
  }


  @Override
  public boolean isAtomic() {
    return true;
  }

  @Override
  protected void _setInt(int offset, int value) {
    intAdders[offset].reset();
    intAdders[offset].add(value);
  }

  @Override
  protected void _setLong(int offset, long value) {
    longAdders[offset].reset();
    longAdders[offset].add(value);
  }

  @Override
  protected void _setDouble(int offset, double value) {
    doubleAdders[offset].reset();
    doubleAdders[offset].add(value);
  }

  @Override
  protected int _getInt(int offset) {
    return intAdders[offset].intValue();
  }

  @Override
  protected long _getLong(int offset) {
    return longAdders[offset].sum();
  }

  @Override
  protected double _getDouble(int offset) {
    return doubleAdders[offset].sum();
  }

  @Override
  protected void _incInt(int offset, int delta) {
    intAdders[offset].add(delta);
  }

  @Override
  protected void _incLong(int offset, long delta) {
    longAdders[offset].add(delta);
  }

  @Override
  protected void _incDouble(int offset, double delta) {
    doubleAdders[offset].add(delta);
  }
}
