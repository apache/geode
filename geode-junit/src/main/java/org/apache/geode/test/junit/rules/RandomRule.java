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
package org.apache.geode.test.junit.rules;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.security.SecureRandom;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.test.junit.rules.serializable.SerializableStatement;
import org.apache.geode.test.junit.rules.serializable.SerializableTestRule;

@SuppressWarnings({"serial", "unused", "WeakerAccess", "NumericCastThatLosesPrecision"})
public class RandomRule extends SecureRandom implements GsRandom, SerializableTestRule {

  private final transient AtomicReference<SecureRandom> random = new AtomicReference<>();
  private final byte[] seed;

  public RandomRule() {
    this(null, null);
  }

  public RandomRule(byte[] seed) {
    this(null, seed);
  }

  public RandomRule(SecureRandom random) {
    this(random, null);
  }

  private RandomRule(SecureRandom random, byte[] seed) {
    this.random.set(random);
    this.seed = seed;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return statement(base);
  }

  private Statement statement(Statement base) {
    return new SerializableStatement() {
      @Override
      public void evaluate() throws Throwable {
        before();
        base.evaluate();
      }
    };
  }

  private void before() {
    random.compareAndSet(null, newRandom());
  }

  /**
   * Returns the next pseudorandom, uniformly distributed {@code boolean} value from this random
   * number generator's sequence.
   *
   * @return the next pseudorandom, uniformly distributed {@code boolean} value from this random
   *         number generator's sequence.
   */
  @Override
  public boolean nextBoolean() {
    return next(1) == 0;
  }

  /**
   * Returns the next pseudorandom, uniformly distributed {@code char} value from this random number
   * generator's sequence There is a hack here to prevent '}' so as to eliminate the possibility of
   * generating a sequence which would falsely get marked as a suspect string while we are matching
   * the pattern {@code {[0-9]+}}.
   *
   * @return the next pseudorandom, uniformly distributed {@code char} value from this random number
   *         generator's sequence.
   */
  @Override
  public char nextChar() {
    char c = (char) next(16);
    if (c == '}') {
      c = nextChar(); // prevent right bracket, try again
    }
    return c;
  }

  /**
   * Returns the next pseudorandom, uniformly distributed {@code byte} value from this random number
   * generator's sequence.
   *
   * @return the next pseudorandom, uniformly distributed {@code byte} value from this random
   *         number generator's sequence.
   */
  @Override
  public byte nextByte() {
    return (byte) next(8);
  }

  /**
   * Returns the next pseudorandom, uniformly distributed {@code double} value from this random
   * number generator's sequence within a range from 0 to max.
   *
   * @param max the maximum range (inclusive) for the pseudorandom.
   * @return the next pseudorandom, uniformly distributed {@code double} value from this random
   *         number generator's sequence.
   */
  @Override
  public double nextDouble(double max) {
    return nextDouble(0.0, max);
  }

  /**
   * Returns the next pseudorandom, uniformly distributed {@code double} value from this random
   * number generator's sequence within a range from min to max.
   *
   * @param min the minimum range (inclusive) for the pseudorandom.
   * @param max the maximum range (inclusive) for the pseudorandom.
   * @return the next pseudorandom, uniformly distributed {@code double} value from this random
   *         number generator's sequence.
   */
  @Override
  public double nextDouble(double min, double max) {
    return nextDouble() * (max - min) + min;
  }

  /**
   * Returns the next pseudorandom, uniformly distributed {@code short} value from this random
   * number generator's sequence.
   *
   * @return the next pseudorandom, uniformly distributed {@code short} value from this random
   *         number generator's sequence.
   */
  @Override
  public short nextShort() {
    return (short) nextChar();
  }

  /**
   * Returns the next pseudorandom, uniformly distributed {@code long} value from this random number
   * generator's sequence within a range from 0 to max.
   *
   * @param max the maximum range (inclusive) for the pseudorandom.
   * @return the next pseudorandom, uniformly distributed {@code long} value from this random number
   *         generator's sequence.
   */
  @Override
  public long nextLong(long max) {
    if (max == Long.MAX_VALUE) {
      max--;
    }
    return Math.abs(nextLong()) % (max + 1);
  }

  /**
   * Returns the next pseudorandom, uniformly distributed {@code long} value from this random number
   * generator's sequence within a range from min to max.
   *
   * @param min the minimum range (inclusive) for the pseudorandom.
   * @param max the maximum range (inclusive) for the pseudorandom.
   * @return the next pseudorandom, uniformly distributed {@code long} value from this random number
   *         generator's sequence.
   */
  @Override
  public long nextLong(long min, long max) {
    return nextLong(max - min) + min;
  }

  /**
   * Returns the next pseudorandom, uniformly distributed {@code int} value from this random number
   * generator's sequence within a range from 0 to max (inclusive -- which is different from
   * {@link Random#nextInt}).
   *
   * @param max the maximum range (inclusive) for the pseudorandom.
   * @return the next pseudorandom, uniformly distributed {@code int} value from this random number
   *         generator's sequence.
   */
  @Override
  public int nextInt(int max) {
    if (max == Integer.MAX_VALUE) {
      max--;
    }

    int theNext = nextInt();
    // Math.abs behaves badly when given min int, so avoid
    if (theNext == Integer.MIN_VALUE) {
      theNext = Integer.MIN_VALUE + 1;
    }
    return Math.abs(theNext) % (max + 1);
  }

  /**
   * Returns the next pseudorandom, uniformly distributed {@code int} value from this random number
   * generator's sequence within a range from min to max. If max < min, returns 0.
   *
   * @param min the minimum range (inclusive) for the pseudorandom.
   * @param max the maximum range (inclusive) for the pseudorandom.
   * @return the next pseudorandom, uniformly distributed {@code int} value from this random number
   *         generator's sequence.
   */
  @Override
  public int nextInt(int min, int max) {
    if (max < min) {
      return 0; // handle max == 0 and avoid divide-by-zero exceptions
    }

    return nextInt(max - min) + min;
  }

  /**
   * Returns the next pseudorandom, uniformly distributed element from the specified iterable as
   * indexed by this random number generator's sequence.
   *
   * @param iterable the range of values (inclusive) for the pseudorandom.
   * @return the next pseudorandom, uniformly distributed element from the specified iterable as
   *         indexed by this random number generator's sequence.
   * @throws NullPointerException if iterable is null.
   * @throws IllegalArgumentException if iterable is empty.
   */
  public <T> T next(Iterable<T> iterable) {
    List<T> list = requireNonNulls(requireNonEmpty(stream(iterable.spliterator(), false)
        .collect(toList())));
    return list.get(nextInt(0, list.size() - 1));
  }

  /**
   * Returns the next pseudorandom, uniformly distributed element from the specified values as
   * indexed by this random number generator's sequence.
   *
   * @param values the range of values (inclusive) for the pseudorandom.
   * @return the next pseudorandom, uniformly distributed element from the specified values as
   *         indexed by this random number generator's sequence.
   * @throws NullPointerException if values is null.
   * @throws IllegalArgumentException if values is empty.
   */
  public <T> T next(T... values) {
    List<T> list = requireNonNulls(requireNonEmpty(asList(values)));
    return requireNonEmpty(list).get(nextInt(0, list.size() - 1));
  }

  @VisibleForTesting
  byte[] getSeed() {
    return seed == null ? null : seed.clone();
  }

  private SecureRandom newRandom() {
    if (seed == null) {
      return new SecureRandom();
    }
    return new SecureRandom(seed);
  }

  private <T> List<T> requireNonEmpty(List<T> list) {
    if (list.isEmpty()) {
      throw new IllegalArgumentException("At least one element is required");
    }
    return list;
  }

  private <T> List<T> requireNonNulls(List<T> list) {
    list.forEach(Objects::requireNonNull);
    return list;
  }

  private void readObject(ObjectInputStream stream) throws InvalidObjectException {
    throw new InvalidObjectException("SerializationProxy required");
  }

  protected Object writeReplace() {
    return new SerializationProxy(this);
  }

  /**
   * Serialization proxy for {@code RandomRule}.
   */
  @SuppressWarnings("serial")
  private static class SerializationProxy implements Serializable {

    private final byte[] seed;

    private SerializationProxy(RandomRule instance) {
      seed = instance.getSeed();
    }

    protected Object readResolve() {
      RandomRule randomRule = seed == null ? new RandomRule() : new RandomRule(seed.clone());
      randomRule.before();
      return randomRule;
    }
  }
}
