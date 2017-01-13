/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef __GS_RANDOM_HPP__
#define __GS_RANDOM_HPP__

#include <gfcpp/gf_base.hpp>

#include <string>

#include "SpinLock.hpp"
#include "MersenneTwister.hpp"

namespace gemfire {
namespace testframework {

class GsRandom {
 private:
  static MTRand gen;
  static GsRandom* singleton;
  static int32_t seedUsed;
  static SpinLock lck;
  static void setInstance(int32_t seed);

  GsRandom() {}
  static void setSeed(int32_t seed);

 public:
  ~GsRandom() {
    if (singleton != NULL) {
      delete singleton;
      singleton = NULL;
    }
  }

  /**
    * Creates a new random number generator. Its seed is initialized to
    * a value based on the /dev/urandom or current time.
    *
    * @see     java.lang.System#currentTimeMillis()
    * @see     java.util.Random#Random()
    */
  inline static GsRandom* getInstance() {
    if (singleton == 0) setInstance(-1);
    return singleton;
  }

  /**
    * Creates a new random number generator using a single
    * <code>int32_t</code> seed.
    *
    * @param   seed   the initial seed.
    * @see     java.util.Random#Random(int32_t)
    */
  static GsRandom* getInstance(int32_t seed);

  /**
    * @return the next pseudorandom, uniformly distributed <code>boolean</code>
    *         value from this random number generator's sequence.
    */
  inline bool nextBoolean() { return (singleton->gen.randInt(1) == 0); }

  /**
    * @return the next pseudorandom, uniformly distributed <code>uint16_t</code>
    *         value from this random number generator's sequence.
    */
  inline uint16_t nextInt16() {
    return (uint16_t)singleton->gen.randInt(0xffff);
  }

  /**
    * @return the next pseudorandom, uniformly distributed <code>byte</code>
    *         value from this random number generator's sequence.
    */
  inline uint8_t nextByte() { return (uint8_t)singleton->gen.randInt(0xff); }

  /**
    * @param   min the minimum range (inclusive) for the pseudorandom.
    * @param   max the maximum range (inclusive) for the pseudorandom.
    * @return  the next pseudorandom, uniformly distributed <code>char</code>
    *          value from this random number generator's sequence.
    *       If max < min, returns 0 .
    */
  inline uint8_t nextByte(int32_t min, int32_t max) {
    if (max < min) return 0;
    return (uint8_t)(singleton->gen.randInt(max - min) + min);
  }

  /**
    * @param   max the maximum range (inclusive) for the pseudorandom.
    * @return  the next pseudorandom, uniformly distributed <code>double</code>
    *          value from this random number generator's sequence.
    */
  inline double nextDouble(double max) { return nextDouble(0.0, max); }

  /**
    * @param   min the minimum range (inclusive) for the pseudorandom.
    * @param   max the maximum range (inclusive) for the pseudorandom.
    * @return  the next pseudorandom, uniformly distributed <code>double</code>
    *      value from this random number generator's sequence within a range
    *      from min to max.
    */
  inline double nextDouble(double min, double max) {
    return (double)(singleton->gen.rand(max - min) + min);
  }

  /**
    * @param   max the maximum range (inclusive) for the pseudorandom.
    * @return  the next pseudorandom, uniformly distributed <code>int32_t</code>
    *          value from this random number generator's sequence.
    */
  inline int32_t nextInt(int32_t max) { return nextInt(0, max); }

  /**
    * @param   min the minimum range (inclusive) for the pseudorandom.
    * @param   max the maximum range (inclusive) for the pseudorandom.
    * @return  the next pseudorandom, uniformly distributed <code>int32_t</code>
    *          value from this random number generator's sequence.
    *       If max < min, returns 0 .
    */
  inline int32_t nextInt(int32_t min, int32_t max) {
    if (max < min) return 0;
    return singleton->gen.randInt(max - min) + min;
  }

  /** @brief return random number where: min <= retValue < max */
  static uint32_t random(uint32_t min, uint32_t max) {
    return (uint32_t)(GsRandom::getInstance()->nextInt(min, max - 1));
  }

  /** @brief return random number where: 0 <= retValue < max */
  static uint32_t random(uint32_t max) {
    return (uint32_t)(GsRandom::getInstance()->nextInt(0, max - 1));
  }

  /** @brief return random double where: min <= retValue <= max */
  static double random(double min, double max) {
    return GsRandom::getInstance()->nextDouble(min, max);
  }

  /** @brief return bounded random string,
    * Like randomString(), but returns only only alphanumeric,
    *   underscore, or space characters.
    *
    * @param uSize the length of the random string to generate.
    * @retval a bounded random string
    */
  static std::string getAlphanumericString(uint32_t size) {
    std::string str(size + 1, '\0');
    static const char chooseFrom[] =
        "0123456789 abcdefghijklmnopqrstuvwxyz_ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    static const int32_t chooseSize = sizeof(chooseFrom) - 1;

    for (uint32_t idx = 0; idx < size; idx++)
      str[idx] = chooseFrom[random(chooseSize)];

    return str;
  }

  /** @brief return bounded random string,
    * Like randomString(), but returns only only alphanumeric,
    *   underscore, or space characters.
    *
    * @param uSize the length of the random string to generate.
    * @retval a bounded random string
    */
  static void getAlphanumericString(uint32_t size, char* buffer) {
    static const char chooseFrom[] =
        "0123456789 abcdefghijklmnopqrstuvwxyz_ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    static const int32_t chooseSize = sizeof(chooseFrom) - 1;

    for (uint32_t idx = 0; idx < size; idx++)
      buffer[idx] = chooseFrom[random(chooseSize)];
  }

  //  /**
  //  * Returns a randomly-selected element of Vector vec.
  //  */
  //  inline void * randomElement(Vector vec)
  //  {
  //    return (void *)(vec.at(nextInt(vec.size())));
  //  }

  /**
    * @param max the maximum length of the random string to generate.
    * @return a bounded random string with a length between 0 and
    * max length inclusive.
    */
  char* randomString(int32_t max, int32_t min = 0);

  /**
    * Like randomString(), but returns only readable characters.
  *
    * @param max the maximum length of the random string to generate.
    * @return a bounded random string with a length between 0 and
    * max length inclusive.
    */
  char* randomReadableString(int32_t max, int32_t min = 0);

  /**
    * Like randomString(), but returns only alphanumeric, underscore, or space
   * characters.
  *
    * @param max the maximum length of the random string to generate.
    * @return a bounded random string with a length between 0 and
    * max length inclusive.
    */
  char* randomAlphanumericString(int32_t max, int32_t min,
                                 const char* prefix = 0);
};

}  // namespace testframework
}  // namespace gemfire
#endif  // __GS_RANDOM_HPP__
