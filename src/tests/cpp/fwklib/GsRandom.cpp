/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "GsRandom.hpp"

using namespace gemfire;
using namespace gemfire::testframework;

GsRandom *GsRandom::singleton = 0;
MTRand GsRandom::gen;
int32_t GsRandom::seedUsed = -101;
SpinLock GsRandom::lck;

/**
  * Creates a new random number generator using a single
  * <code>int32_t</code> seed.
  *
  * @param   seed   the initial seed.
  * @see     java.util.Random#Random(int32_t)
  */
GsRandom *GsRandom::getInstance(int32_t seed) {
  if (singleton == 0) {
    setInstance(seed);
  } else {
    SpinLockGuard guard(lck);
    setSeed(seed);
  }
  return singleton;
}

void GsRandom::setInstance(int32_t seed) {
  SpinLockGuard guard(lck);
  if (singleton == 0) {
    singleton = new GsRandom();
    if (seed != -1) {
      singleton->gen.seed(seed);
    } else {
      singleton->gen.seed();
    }
    seedUsed = seed;
  }
}

void GsRandom::setSeed(int32_t seed) {
  if (seed != seedUsed) {
    if (seed != -1) {
      singleton->gen.seed(seed);
    } else {
      singleton->gen.seed();
    }
    seedUsed = seed;
  }
}

/**
* @param max the maximum length of the random string to generate.
* @param min the minimum length of the random string to generate, default is 0.
* @return a bounded random string with a length between min and
* max length inclusive.
*/
char *GsRandom::randomString(int32_t max, int32_t min) {
  int32_t len = (max == min) ? max : nextInt(min, max);
  char *buf = reinterpret_cast<char *>(malloc(len + 1));
  for (int32_t i = 0; i < len; i++) buf[i] = nextByte();
  buf[len] = 0;
  return buf;
}

/**
* Like randomString(), but returns only readable characters.
*
* @param max the maximum length of the random string to generate.
* @param min the minimum length of the random string to generate, default is 0.
* @return a bounded random string with a length between min and
* max length inclusive.
*/
char *GsRandom::randomReadableString(int32_t max, int32_t min) {
  int32_t len = (max == min) ? max : nextInt(min, max);
  char *buf = reinterpret_cast<char *>(malloc(len + 1));
  for (int32_t i = 0; i < len; i++) buf[i] = nextByte(32, 126);
  buf[len] = 0;
  return buf;
}

const char choseFrom[] =
    "0123456789 abcdefghijklmnopqrstuvwxyz_ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const int32_t choseSize = static_cast<int32_t>(strlen(choseFrom)) - 1;
/**
* Like randomString(), but returns only only alphanumeric, underscore, or space
* characters.
*
* @param max the maximum length of the random string to generate.
* @param min the minimum length of the random string to generate, default is 0.
* @return a bounded random string with a length between min and
* max length inclusive.
*/
char *GsRandom::randomAlphanumericString(int32_t max, int32_t min,
                                         const char *prefix) {
  int32_t len = (max == min) ? max : nextInt(min, max);
  char *buf = reinterpret_cast<char *>(malloc(len + 1));
  for (int32_t i = 0; i < len; i++) buf[i] = choseFrom[nextByte(0, choseSize)];
  buf[len] = 0;
  return buf;
}
