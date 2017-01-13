/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
// MersenneTwister.h
// Mersenne Twister random number generator -- a C++ class MTRand
// Based on code by Makoto Matsumoto, Takuji Nishimura, and Shawn Cokus
// Richard J. Wagner  v1.0  15 May 2003  rjwagner@writeme.com

// The Mersenne Twister is an algorithm for generating random numbers.  It
// was designed with consideration of the flaws in various other generators.
// The period, 2^19937-1, and the order of equidistribution, 623 dimensions,
// are far greater.  The generator is also fast; it avoids multiplication and
// division, and it benefits from caches and pipelines.  For more information
// see the inventors' web page at http://www.math.keio.ac.jp/~matumoto/emt.html

// Reference
// M. Matsumoto and T. Nishimura, "Mersenne Twister: A 623-Dimensionally
// Equidistributed Uniform Pseudo-Random Number Generator", ACM Transactions on
// Modeling and Computer Simulation, Vol. 8, No. 1, January 1998, pp 3-30.

// Copyright (C) 1997 - 2002, Makoto Matsumoto and Takuji Nishimura,
// Copyright (C) 2000 - 2003, Richard J. Wagner
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
//   1. Redistributions of source code must retain the above copyright
//      notice, this list of conditions and the following disclaimer.
//
//   2. Redistributions in binary form must reproduce the above copyright
//      notice, this list of conditions and the following disclaimer in the
//      documentation and/or other materials provided with the distribution.
//
//   3. The names of its contributors may not be used to endorse or promote
//      products derived from this software without specific prior written
//      permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER
// OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
// EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
// PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
// LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

// The original code included the following notice:
//
//     When you use this, send an email to: matumoto@math.keio.ac.jp
//     with an appropriate reference to your work.
//
// It would be nice to CC: rjwagner@writeme.com and Cokus@math.washington.edu
// when you write.

#ifndef MERSENNETWISTER_H
#define MERSENNETWISTER_H

// Not thread safe (unless auto-initialization is avoided and each thread has
// its own MTRand object)

#include <iostream>
#include <limits.h>
#include <stdio.h>
#include <time.h>
#include <math.h>

#include "SpinLock.hpp"
#include <gfcpp/gf_base.hpp>

class MTRand {
  // Data
 public:
  enum { N = 624 };       // length of state vector
  enum { SAVE = N + 1 };  // length of array for save()

 protected:
  enum { M = 397 };  // period parameter

  uint32_t state[N];  // internal state
  uint32_t* pNext;    // next value to get from state
  int32_t left;       // number of values left before reload needed

 private:
  static gemfire::SpinLock lck;

  // Methods
 public:
  MTRand(const uint32_t& oneSeed);  // initialize with a simple uint32_t
  MTRand(uint32_t* const bigSeed,
         uint32_t const seedLength = N);  // or an array
  MTRand();  // auto-initialize with /dev/urandom or time() and clock()

  // Do NOT use for CRYPTOGRAPHY without securely hashing several returned
  // values together, otherwise the generator state can be learned after
  // reading 624 consecutive values.

  // Access to 32-bit random numbers
  double rand();                          // real number in [0,1]
  double rand(const double& n);           // real number in [0,n]
  double randExc();                       // real number in [0,1)
  double randExc(const double& n);        // real number in [0,n)
  double randDblExc();                    // real number in (0,1)
  double randDblExc(const double& n);     // real number in (0,n)
  uint32_t randInt();                     // integer in [0,2^32-1]
  uint32_t randInt(const uint32_t& n);    // integer in [0,n] for n < 2^32
  double operator()() { return rand(); }  // same as rand()

  // Access to 53-bit random numbers (capacity of IEEE double precision)
  double rand53();  // real number in [0,1)

  // Access to nonuniform random number distributions
  double randNorm(const double& mean = 0.0, const double& variance = 0.0);

  // Re-seeding functions with same behavior as initializers
  void seed(const uint32_t oneSeed);
  void seed(uint32_t* const bigSeed, const uint32_t seedLength = N);
  void seed();

  // Saving and loading generator state
  void save(uint32_t* saveArray) const;  // to array of size SAVE
  void load(uint32_t* const loadArray);  // from such array
  friend std::ostream& operator<<(std::ostream& os, const MTRand& mtrand);
  friend std::istream& operator>>(std::istream& is, MTRand& mtrand);

 protected:
  void initialize(const uint32_t oneSeed);
  void reload();
  uint32_t hiBit(const uint32_t& u) const { return u & 0x80000000; }
  uint32_t loBit(const uint32_t& u) const { return u & 0x00000001; }
  uint32_t loBits(const uint32_t& u) const { return u & 0x7fffffff; }
  uint32_t mixBits(const uint32_t& u, const uint32_t& v) const {
    return hiBit(u) | loBits(v);
  }
  uint32_t twist(const uint32_t& m, const uint32_t& s0,
                 const uint32_t& s1) const {
    return m ^ (mixBits(s0, s1) >> 1) ^ (-(int32_t)(loBit(s1)) & 0x9908b0df);
  }
  static uint32_t hash(time_t t, clock_t c);
};

#endif  // MERSENNETWISTER_H

// Change log:
//
// v0.1 - First release on 15 May 2000
//      - Based on code by Makoto Matsumoto, Takuji Nishimura, and Shawn Cokus
//      - Translated from C to C++
//      - Made completely ANSI compliant
//      - Designed convenient interface for initialization, seeding, and
//        obtaining numbers in default or user-defined ranges
//      - Added automatic seeding from /dev/urandom or time() and clock()
//      - Provided functions for saving and loading generator state
//
// v0.2 - Fixed bug which reloaded generator one step too late
//
// v0.3 - Switched to clearer, faster reload() code from Matthew Bellew
//
// v0.4 - Removed trailing newline in saved generator format to be consistent
//        with output format of built-in types
//
// v0.5 - Improved portability by replacing static const int's with enum's and
//        clarifying return values in seed(); suggested by Eric Heimburg
//      - Removed MAXINT constant; use 0xffffffffUL instead
//
// v0.6 - Eliminated seed overflow when uint32_t is larger than 32 bits
//      - Changed integer [0,n] generator to give better uniformity
//
// v0.7 - Fixed operator precedence ambiguity in reload()
//      - Added access for real numbers in (0,1) and (0,n)
//
// v0.8 - Included time.h header to properly support time_t and clock_t
//
// v1.0 - Revised seeding to match 26 Jan 2002 update of Nishimura and Matsumoto
//      - Allowed for seeding with arrays of any length
//      - Added access for real numbers in [0,1) with 53-bit resolution
//      - Added access for real numbers from normal (Gaussian) distributions
//      - Increased overall speed by optimizing twist()
//      - Doubled speed of integer [0,n] generation
//      - Fixed out-of-range number generation on 64-bit machines
//      - Improved portability by substituting literal constants for long enum's
//      - Changed license from GNU LGPL to BSD
