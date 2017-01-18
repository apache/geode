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

#include "GsRandom.hpp"

using namespace apache::geode::client;
using namespace apache::geode::client::testframework;

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
