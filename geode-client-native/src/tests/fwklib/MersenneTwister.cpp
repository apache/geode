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
// A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
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

// Not thread safe (unless auto-initialization is avoided and each thread has
// its own MTRand object)

#include "MersenneTwister.hpp"

gemfire::SpinLock MTRand::lck;

 MTRand::MTRand( const uint32_t& oneSeed )
  { seed(oneSeed); }

 MTRand::MTRand( uint32_t *const bigSeed, const uint32_t seedLength )
  { seed(bigSeed,seedLength); }

 MTRand::MTRand()
  { seed(); }

 double MTRand::rand()
  { return double(randInt()) * (1.0/4294967295.0); }

 double MTRand::rand( const double& n )
  { return rand() * n; }

 double MTRand::randExc()
  { return double(randInt()) * (1.0/4294967296.0); }

 double MTRand::randExc( const double& n )
  { return randExc() * n; }

 double MTRand::randDblExc()
  { return ( double(randInt()) + 0.5 ) * (1.0/4294967296.0); }

 double MTRand::randDblExc( const double& n )
  { return randDblExc() * n; }

 double MTRand::rand53()
{
  uint32_t a = randInt() >> 5, b = randInt() >> 6;
  return ( a * 67108864.0 + b ) * (1.0/9007199254740992.0);  // by Isaku Wada
}

 double MTRand::randNorm( const double& mean, const double& variance )
{
  // Return a real number from a normal (Gaussian) distribution with given
  // mean and variance by Box-Muller method
  double r = sqrt( -2.0 * log( 1.0-randDblExc()) ) * variance;
  double phi = 2.0 * 3.14159265358979323846264338328 * randExc();
  return mean + r * cos(phi);
}

 uint32_t MTRand::randInt()
{
  // Pull a 32-bit integer from the generator state
  // Every other access function simply transforms the numbers extracted here
  gemfire::SpinLockGuard guard( lck );
  
  if( left <= 0 ) reload();
  --left;
  register uint32_t s1;
  s1 = *pNext++;
  s1 ^= (s1 >> 11);
  s1 ^= (s1 <<  7) & 0x9d2c5680UL;
  s1 ^= (s1 << 15) & 0xefc60000UL;
  return ( s1 ^ (s1 >> 18) );
}

 uint32_t MTRand::randInt( const uint32_t& n )
{
  // Find which bits are used in n
  // Optimized by Magnus Jonsson (magnus@smartelectronix.com)
  uint32_t used = n;
  used |= used >> 1;
  used |= used >> 2;
  used |= used >> 4;
  used |= used >> 8;
  used |= used >> 16;
  
  // Draw numbers until one is found in [0,n]
  uint32_t i;
  do
    i = randInt() & used;  // toss unused bits to shorten search
  while( i > n );
  return i;
}


 void MTRand::seed( const uint32_t oneSeed )
{
  // Seed the generator with a simple uint32_t
  initialize(oneSeed);
  reload();
}


 void MTRand::seed( uint32_t *const bigSeed, const uint32_t seedLength )
{
  // Seed the generator with an array of uint32_t's
  // There are 2^19937-1 possible initial states.  This function allows
  // all of those to be accessed by providing at least 19937 bits (with a
  // default seed length of N = 624 uint32_t's).  Any bits above the lower 32
  // in each element are discarded.
  // Just call seed() if you want to get array from /dev/urandom
  initialize(19650218UL);
  register int32_t i = 1;
  register uint32_t j = 0;
  register int32_t k = ( N > seedLength ? N : seedLength );
  for( ; k; --k )
  {
    state[i] =
      state[i] ^ ( (state[i-1] ^ (state[i-1] >> 30)) * 1664525UL );
    state[i] += ( bigSeed[j] & 0xffffffffUL ) + j;
    state[i] &= 0xffffffffUL;
    ++i;  ++j;
    if( i >= N ) { state[0] = state[N-1];  i = 1; }
    if( j >= seedLength ) j = 0;
  }
  for( k = N - 1; k; --k )
  {
    state[i] =
      state[i] ^ ( (state[i-1] ^ (state[i-1] >> 30)) * 1566083941UL );
    state[i] -= i;
    state[i] &= 0xffffffffUL;
    ++i;
    if( i >= N ) { state[0] = state[N-1];  i = 1; }
  }
  state[0] = 0x80000000UL;  // MSB is 1, assuring non-zero initial array
  reload();
}


 void MTRand::seed()
{
  // Seed the generator with an array from /dev/urandom if available
  // Otherwise use a hash of time() and clock() values
  
  // First try getting an array from /dev/urandom
  FILE* urandom = fopen( "/dev/urandom", "rb" );
  if( urandom )
  {
    uint32_t bigSeed[N];
    register uint32_t *s = bigSeed;
    register int32_t i = N;
    register int32_t success = 1;
    while( success && i-- )
      success = (int32_t) fread( s++, sizeof(uint32_t), 1, urandom );
    fclose(urandom);
    if( success ) { seed( bigSeed, N );  return; }
  }
  
  // Was not successful, so use time() and clock() instead
  seed( hash( time(NULL), clock() ) );
}


 void MTRand::initialize( const uint32_t seed )
{
  // Initialize generator state with seed
  // See Knuth TAOCP Vol 2, 3rd Ed, p.106 for multiplier.
  // In previous versions, most significant bits (MSBs) of the seed affect
  // only MSBs of the state array.  Modified 9 Jan 2002 by Makoto Matsumoto.
  register uint32_t *s = state;
  register uint32_t *r = state;
  register int32_t i = 1;
  *s++ = seed & 0xffffffffUL;
  for( ; i < N; ++i )
  {
    *s++ = ( 1812433253UL * ( *r ^ (*r >> 30) ) + i ) & 0xffffffffUL;
    r++;
  }
}


 void MTRand::reload()
{
  // Generate N new values in state
  // Made clearer and faster by Matthew Bellew (matthew.bellew@home.com)
  register uint32_t *p = state;
  register int32_t i;
  for( i = N - M; i--; ++p )
    *p = twist( p[M], p[0], p[1] );
  for( i = M; --i; ++p )
    *p = twist( p[M-N], p[0], p[1] );
  *p = twist( p[M-N], p[0], state[0] );

  left = N, pNext = state;
}


 uint32_t MTRand::hash( time_t t, clock_t c )
{
  // Get a uint32_t from t and c
  // Better than uint32_t(x) in case x is floating point in [0,1]
  // Based on code by Lawrence Kirby (fred@genesis.demon.co.uk)

  static uint32_t differ = 0;  // guarantee time-based seeds will change

  uint32_t h1 = 0;
  unsigned char *p = (unsigned char *) &t;
  for( size_t i = 0; i < sizeof(t); ++i )
  {
    h1 *= UCHAR_MAX + 2U;
    h1 += p[i];
  }
  uint32_t h2 = 0;
  p = (unsigned char *) &c;
  for( size_t j = 0; j < sizeof(c); ++j )
  {
    h2 *= UCHAR_MAX + 2U;
    h2 += p[j];
  }
  return ( h1 + differ++ ) ^ h2;
}


 void MTRand::save( uint32_t* saveArray ) const
{
  register uint32_t *sa = saveArray;
  register const uint32_t *s = state;
  register int32_t i = N;
  for( ; i--; *sa++ = *s++ ) {}
  *sa = left;
}


 void MTRand::load( uint32_t *const loadArray )
{
  register uint32_t *s = state;
  register uint32_t *la = loadArray;
  register int32_t i = N;
  for( ; i--; *s++ = *la++ ) {}
  left = *la;
  pNext = &state[N-left];
}


 std::ostream& operator<<( std::ostream& os, const MTRand& mtrand )
{
  register const uint32_t *s = mtrand.state;
  register int32_t i = mtrand.N;
  for( ; i--; os << *s++ << "\t" ) {}
  return os << mtrand.left;
}


 std::istream& operator>>( std::istream& is, MTRand& mtrand )
{
  register uint32_t *s = mtrand.state;
  register int32_t i = mtrand.N;
  for( ; i--; is >> *s++ ) {}
  is >> mtrand.left;
  mtrand.pNext = &mtrand.state[mtrand.N-mtrand.left];
  return is;
}

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
//      - Improved portability by substituting literal constants for int32_t enum's
//      - Changed license from GNU LGPL to BSD
