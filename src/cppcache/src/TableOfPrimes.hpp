#pragma once

#ifndef GEODE_TABLEOFPRIMES_H_
#define GEODE_TABLEOFPRIMES_H_

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

#include <gfcpp/gfcpp_globals.hpp>
#include <algorithm>

namespace {
static const uint32_t g_primeTable[] = {
    53,        97,           193,         389,       769,       1543,
    3079,      6151,         12289,       24593,     49157,     98317,
    196613,    393241,       786433,      1572869,   3145739,   6291469,
    12582917,  25165843,     50331653,    100663319, 201326611, 402653189,
    805306457, 1610612741UL, 3221225473UL};
static const uint32_t g_primeLen = sizeof(g_primeTable) / sizeof(uint32_t);

static const uint8_t g_primeConcurTable[] = {
    2,   3,   5,   7,   11,  13,  17,  19,  23,  29,  31,  37,  41,  43,
    47,  53,  59,  61,  67,  71,  73,  79,  83,  89,  97,  101, 103, 107,
    109, 113, 127, 131, 137, 139, 149, 151, 157, 163, 167, 173, 179, 181,
    191, 193, 197, 199, 211, 223, 227, 229, 233, 239, 241, 251};
static const uint8_t g_primeConcurLen =
    static_cast<uint8_t>(sizeof(g_primeConcurTable) / sizeof(uint8_t));

}  // anonymous namespace

namespace apache {
namespace geode {
namespace client {
/** @brief find a prime number greater than a given integer.
 *  A sampling of primes are used from 0 to 1 million. Not every prime is
 *  necessary, as the map scales, little steps are usually uninteresting.
 */
class CPPCACHE_EXPORT TableOfPrimes {
 public:
  inline static uint32_t getPrimeLength() { return g_primeLen; }

  inline static uint32_t getPrime(uint32_t index) {
    if (index < g_primeLen) {
      return g_primeTable[index];
    }
    throw OutOfRangeException("getPrime: index beyond size of prime table");
  }

  static uint32_t nextLargerPrime(uint32_t val, uint32_t& resIndex) {
    const uint32_t* tableEnd = g_primeTable + g_primeLen;
    const uint32_t* idxPtr = std::lower_bound(g_primeTable, tableEnd, val);
    if (idxPtr != tableEnd) {
      resIndex = static_cast<uint32_t>(idxPtr - g_primeTable);
      return *idxPtr;
    }
    throw OutOfRangeException(
        "nextLargerPrime: could not find a prime "
        "number that large");
  }

  inline static uint8_t getMaxPrimeForConcurrency() {
    return g_primeConcurTable[g_primeConcurLen - 1];
  }

  static uint8_t nextLargerPrimeForConcurrency(uint8_t val) {
    const uint8_t* tableEnd = g_primeConcurTable + g_primeConcurLen;
    const uint8_t* idxPtr = std::lower_bound(g_primeConcurTable, tableEnd, val);
    if (idxPtr != tableEnd) {
      return *idxPtr;
    }
    throw OutOfRangeException(
        "nextLargerPrimeForConcurrency: could not "
        "find a prime number that large");
  }
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_TABLEOFPRIMES_H_
