/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hydra;

import java.io.*;
import java.util.*;

/**
  * This is a simple extension of java.util.Random that allows for the
  * generation of random numbers within ranges. It also allows for the
  * generation of random strings (within ranges as well).
  * @see     java.lang.Math#random()
  * @see     java.util.Random
  * @since   JDK1.0
  */

public class GsRandom extends java.util.Random implements Serializable {
  
  /**
    *
    * ourString is a privately held instance of a String with 
    * with some junk characters 
    *
    */
  
  static protected String ourString = "854ku45Q985a.lsdk;,.ifpq4z58Ao45u.sdflkjsdgkjqwJKL:EIUR[p4pnm,.zxc239*h1@0*Fn/~5.+3&gwNa(.3K-c/2bd(kb1.(=wvz!/56NIwk-4/(#mDhn%kd#9jas9_n!KC0-c>3*(fbn3Fl)Fhaw.2?nz~l;1q3=Fbak1>ah1Bci23fripB319v*bnFl2Ba-cH$lfb?A)_2bgFo2_+Vv$al+b124kasbFV[2G}b@9ASFbCk2.KIhb4K";

  /** 
    * Creates a new random number generator. Its seed is initialized to 
    * a value based on the current time. 
    *
    * @see     java.lang.System#currentTimeMillis()
    * @see     java.util.Random#Random()
    */
  
  public GsRandom() {
    super();
  }
  
  /** 
    * Creates a new random number generator using a single 
    * <code>long</code> seed. 
    *
    * @param   seed   the initial seed.
    * @see     java.util.Random#Random(long)
    */

  public GsRandom(long seed) {
    super(seed);
  }

  /**
    * Returns the next pseudorandom, uniformly distributed <code>boolean</code>
    * value from this random number generator's sequence
    *
    * @return the next pseudorandom, uniformly distributed <code>boolean</code>
    *         value from this random number generator's sequence.
    */

  public boolean nextBoolean() {

    return (this.next(1) == 0);
  }

  /**
    * Returns the next pseudorandom, uniformly distributed <code>char</code>
    * value from this random number generator's sequence
    * There is a hack here to prevent '}' so as to eliminate the possiblity 
    * of generating a sequence which would falsely get marked as a suspect
    * string while we are matching the pattern <code>{[0-9]+}</code>.
    * @return the next pseudorandom, uniformly distributed <code>char</code>
    *         value from this random number generator's sequence.
    */

  public char nextChar() {

    char c = (char) this.next(16);
    if( c == '}' ) c = nextChar(); //prevent right bracket, try again
    return c;
  }

  /**
    * Returns the next pseudorandom, uniformly distributed <code>byte</code>
    * value from this random number generator's sequence
    *
    * @return the next pseudorandom, uniformly distributed <code>byte</code>
    *         value from this random number generator's sequence.
    */

  public byte nextByte() {

    return (byte) this.next(8);
  }

  /**
    * Returns the next pseudorandom, uniformly distributed <code>double</code>
    * value from this random number generator's sequence within a range
    * from 0 to max.
    *
    * @param   max the maximum range (inclusive) for the pseudorandom.
    * @return  the next pseudorandom, uniformly distributed <code>double</code>
    *          value from this random number generator's sequence.
    */
  public double nextDouble(double max) {

    return nextDouble(0.0, max);

  } 

  /**
    * Returns the next pseudorandom, uniformly distributed <code>double</code>
    * value from this random number generator's sequence within a range
    * from min to max.
    *
    * @param   min the minimum range (inclusive) for the pseudorandom.
    * @param   max the maximum range (inclusive) for the pseudorandom.
    * @return  the next pseudorandom, uniformly distributed <code>double</code>
    *          value from this random number generator's sequence.
    */

  public double nextDouble(double min, double max) {

    return nextDouble() * (max - min) + min;

    // return nextDouble(max-min) + min;
  }

  public short nextShort() {
    return (short) this.nextChar();
  }

  /**
    * Returns the next pseudorandom, uniformly distributed <code>long</code>
    * value from this random number generator's sequence within a range
    * from 0 to max.
    *
    * @param   max the maximum range (inclusive) for the pseudorandom.
    * @return  the next pseudorandom, uniformly distributed <code>long</code>
    *          value from this random number generator's sequence.
    */

  public long nextLong(long max) {

    if (max == Long.MAX_VALUE) {
      max--;
    }

    return Math.abs(this.nextLong()) % (max+1);
  }

  /**
    * Returns the next pseudorandom, uniformly distributed <code>long</code>
    * value from this random number generator's sequence within a range
    * from min to max.
    *
    * @param   min the minimum range (inclusive) for the pseudorandom.
    * @param   max the maximum range (inclusive) for the pseudorandom.
    * @return  the next pseudorandom, uniformly distributed <code>long</code>
    *          value from this random number generator's sequence.
    */

  public long nextLong(long min, long max) {


    return nextLong(max-min) + min;
  }

  /**
    * Returns the next pseudorandom, uniformly distributed <code>int</code>
    * value from this random number generator's sequence within a range
    * from 0 to max (inclusive -- which is different from {@link
    * Random#nextInt}).
    *
    * @param   max the maximum range (inclusive) for the pseudorandom.
    * @return  the next pseudorandom, uniformly distributed <code>int</code>
    *          value from this random number generator's sequence.
    */

  public int nextInt(int max) {

    if (max == Integer.MAX_VALUE) {
      max--;
    }
    
    int theNext = this.nextInt();
    // Math.abs behaves badly when given min int, so avoid
    if (theNext == Integer.MIN_VALUE) {
        theNext = Integer.MIN_VALUE + 1;
    }
    return Math.abs(theNext) % (max+1);
  }

  /**
    * Returns the next pseudorandom, uniformly distributed <code>int</code>
    * value from this random number generator's sequence within a range
    * from min to max.
    * If max < min, returns 0 .
    *
    * @param   min the minimum range (inclusive) for the pseudorandom.
    * @param   max the maximum range (inclusive) for the pseudorandom.
    * @return  the next pseudorandom, uniformly distributed <code>int</code>
    *          value from this random number generator's sequence.
    */

  public int nextInt(int min, int max) {
    if (max < min)
      return 0;  // handle max == 0 and avoid  divide-by-zero exceptions

    return nextInt(max-min) + min;
  }

  /**
    * Returns a large, pregenerated string.
    *
    * @return a large, pregenerated string.
    */

  private String string() {
    return ourString;
  }

  /**
    *
    * Returns a random Date.
    * 
    * @return  A random Date.
    */
 
  public Date nextDate() {
    return new Date(nextLong());
  }
 
  /** 
  *
  * Returns a randomly-selected element of Vector vec. 
  *
  */
  public Object randomElement(Vector vec) {
    Object result;
    synchronized (vec) {                           // fix 26810
      int index =  nextInt(0, vec.size() - 1);
      result = vec.elementAt(index);
    }
    return result;
  }

  /**
    * Returns a random subset of a pregenerated string. Both the
    * length and offset of the string are pseudorandom values.
    *
    * @return a random subset of a pregenerated string.
    */

  public String randomString() {

    return this.randomString(this.string().length());
  }

  /**
    * Returns a bounded random subset of a pregenerated large
    * string. The length can be no longer than max. max must be no
    * longer than the length of the pregenerated string.
    *
    * @param max the maximum length of the random string to generate.
    * @return a bounded random string with a length between 0 and
    * max length inclusive.
    */

  public String randomString(int max) {

    int length = this.nextInt(0, max);
    byte[] bytes = new byte[length];
    this.nextBytes(bytes);
    return new String(bytes);
  }

  /**
    * 
    * Like randomString(), but returns only readable characters.
    *
    */
  public String randomReadableString(int max) {

    int stringlen = this.string().length();
    if ( max > stringlen )
      throw new HydraRuntimeException
      (
        "GsRandom.randomReadableString is limited to " + stringlen +
        " characters, cannot create string of length " + max
      );

    int length = this.nextInt(0, max);
    int offset = this.nextInt(0, stringlen - length);
    return this.string().substring(offset, offset+length);
  }
}
