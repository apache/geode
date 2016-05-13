/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
// Technical Report on C++ Performance ISO/IEC TR 18015:2006(E)
// © ISO/IEC 2006 — All rights reserved
/*
Simple/naive measurements to give a rough idea of the relative
cost of facilities related to OOP.
This could be fooled/foiled by clever optimizers and by
cache effects.
Run at least three times to ensure that results are repeatable.
Tests:
virtual function
global function called indirectly
nonvirtual member function
global function
inline member function
macro
1st branch of MI
2nd branch of MI
call through virtual base
call of virtual base function
dynamic cast
two-level dynamic cast
typeid()
call through pointer to member
call-by-reference
call-by-value
pass as pointer to function
pass as function object
not yet:
co-variant return
The cost of the loop is not measurable at this precision:
see inline tests
By default do 1000000 iterations to cout
1st optional argument: number of iterations
2nd optional argument: target file name
*/

#include "cppPerf/CppPerf.hpp"

// --------------------------------------------------------------
#include <stdlib.h> // or <cstdlib>
#include <iostream>
#include <fstream>
#include <time.h> // or <ctime>
#include <vector>
#include <typeinfo>
#include "fwklib/FwkExport.hpp"
#include "fwklib/Timer.hpp"
#include <ace/ACE.h>
#include <ace/OS.h>
#include <ace/Task.h>

using namespace gemfire;
using namespace gemfire::testframework;
using namespace gemfire::testframework::perf;
using namespace ISO_IEC;

static CppPerf * g_test = NULL;

// ----------------------------------------------------------------------------

TESTTASK initialize( const char * initArgs ) {
  int32_t result = FWK_SUCCESS;
  if ( g_test == NULL ) {
    FWKINFO( "Initializing cppPerf library." );
    try {
      g_test = new CppPerf( initArgs );
    } catch( const FwkException &ex ) {
      FWKSEVERE( "initialize: caught exception: " << ex.getMessage() );
      result = FWK_SEVERE;
    }
  }
  return result;
}

// ----------------------------------------------------------------------------

TESTTASK finalize() {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "Finalizing cppPerf library." );
  if ( g_test != NULL ) {
    delete g_test;
    g_test = NULL;
  }
  return result;
}

// ----------------------------------------------------------------------------

void CppPerf::checkTest( const char * taskId ) {
  SpinLockGuard guard( m_lck );
  setTask( taskId );
}

// ----------------------------------------------------------------------------

TESTTASK doTestCppPerf( const char * taskId ) {
  g_test->checkTest( taskId );
  return g_test->testCppPerf();
}

// ----------------------------------------------------------------------------

using namespace std;

template<class T> inline T* ti(T* p)
{
  if (typeid(p) == typeid(int*))
    p++;
  return p;
}

int X::st = 0;
void X::f(int a) { x += a; }
void X::g(int a) { x += a; }
void X::h(int a) { st += a; }
void ISO_IEC::f(S* p, int a) { p->x += a; }
void ISO_IEC::g(S* p, int a) { p->x += a; }
void ISO_IEC::h(int a) { glob += a; }

#define OPS 10000000
#define ITERS 10

char RID[1024];

void logit( const char * msg, int64_t nanos, int32_t iter ) {
  static double avg = 0;
  avg += ( double )nanos / ( double )OPS;
  if ( iter ==  ( ITERS - 1 ) ) { 
    avg /= ITERS;
    FWKINFO( "CppSuite:: " << RID << PerfSuite::getSysName() << "|" << PerfSuite::getNodeName() 
              << "|" << msg << "|" << avg );
    avg = 0;
  }
}

void baseline( int64_t nanos, int64_t ops, int32_t iter ) {
  char buf[256];
  sprintf( buf, "CppSuite Baseline Integer Addition: %d :: %lld nanos %lld ops.", iter + 1, nanos, ops );
  FWKINFO( buf );
}

uint32_t dummy( uint32_t v ) {
  return v++;
}

int CppPerf::testCppPerf()
{
  int i, j; // loop variable here for the benefit of non-conforming compilers
  const int n = OPS; // number of iterations
  
  X* px = new X;
  X x;
  S* ps = new S;
  S s;

  ACE_TCHAR tstamp[64];  
  ACE::timestamp( tstamp, 64, 1 );
  sprintf( RID, "%8.8s|%d|%u|", &tstamp[16], ACE_OS::getpid(), ( uint32_t )( ACE_Thread::self() ) );

  HRTimer t;

  FWKINFO( "CppPerf test run begun." );

  int64_t l;
  int32_t k;
  int64_t iters = 1000;
  for ( j = 0; j < 5; j++ ) {
    k = 0;
    t.start();
    for ( l = 0; l < iters; l++ )
      k = dummy( k );
    baseline( t.elapsedNanos(), iters, j );
    iters *= 10;
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      px->f( 1 );
    logit( "virtual px->f(1)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      p[1](ps, 1);
    logit( "ptr-to-fct p[1](ps,1)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      x.f(1);
    logit( "virtual x.f(1)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      p[1](&s, 1);
    logit( "ptr-to-fct p[1](&s,1)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      px->g(1);
    logit( "member px->g(1)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      g(ps, 1);
    logit( "global g(ps,1)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      x.g(1);
    logit( "member x.g(1)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      g(&s, 1);
    logit( "global g(&s,1)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      X::h(1);
    logit( "static X::h(1)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      h(1);
    logit( "global h(1)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      px->k(1);
    logit( "inline px->k(1)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      K(ps, 1);
    logit( "macro K(ps,1)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      x.k(1);
    logit( "inline x.k(1)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      K(&s, 1);
    logit( "macro K(&s,1)", t.elapsedNanos(), j );
  }

  C* pc = new C;
  A* pa = pc;
  B* pb = pc;

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      pc->g(i);
    logit( "base1 member pc->g(i)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      pc->gg(i);
    logit( "base2 member pc->gg(i)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      pa->f(i);
    logit( "base1 virtual pa->f(i)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      pb->ff(i);
    logit( "base2 virtual pb->ff(i)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      cast(pa, pc);
    logit( "base1 down-cast cast(pa,pc)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      cast(pb, pc);
    logit( "base2 down-cast cast(pb,pc)", t.elapsedNanos(), j );
  }
  
  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      cast(pc, pa);
    logit( "base1 up-cast cast(pc,pa)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      cast(pc, pb);
    logit( "base2 up-cast cast(pc,pb)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      cast(pb, pa);
    logit( "base2 cross-cast cast(pb,pa)", t.elapsedNanos(), j );
  }

  CC* pcc = new CC;
  pa = pcc;
  pb = pcc;

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      cast(pa, pcc);
    logit( "base1 down-cast2 cast(pa,pcc)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      cast(pb, pcc);
    logit( "base2 down-cast cast(pb,pcc)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      cast(pcc, pa);
    logit( "base1 up-cast cast(pcc,pa)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      cast(pcc, pb);
    logit( "base2 up-cast2 cast(pcc,pb)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      cast(pb, pa);
    logit( "base2 cross-cast2 cast(pa,pb)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      cast(pa, pb);
    logit( "base1 cross-cast2 cast(pb,pa)", t.elapsedNanos(), j );
  }

  D* pd = new D;
  pa = pd;

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      pd->g(i);
    logit( "vbase member pd->gg(i)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      pa->f(i);
    logit( "vbase virtual pa->f(i)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      cast(pa, pd);
    logit( "vbase down-cast cast(pa,pd)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      cast(pd, pa);
    logit( "vbase up-cast cast(pd,pa)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      ti(pa);
    logit( "vbase typeid(pa)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      ti(pd);
    logit( "vbase typeid(pd)", t.elapsedNanos(), j );
  }

  void (A::* pmf)(int) = &A::f; // virtual

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      (pa->*pmf)(i);
    logit( "pmf virtual (pa->*pmf)(i)", t.elapsedNanos(), j );
  }

  pmf = &A::g; // non virtual

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      (pa->*pmf)(i);
    logit( "pmf (pa->*pmf)(i)", t.elapsedNanos(), j );
  }

  P pp;

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      by_ref(pp);
    logit( "call by_ref(pp)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      by_val(pp);
    logit( "call by_val(pp)", t.elapsedNanos(), j );
  }

  FO fct;

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      oper(h, glob);
    logit( "call ptr-to-fct oper(h,glob)", t.elapsedNanos(), j );
  }

  for ( j = 0; j < ITERS; j++ ) {
    t.start();
    for ( i = 0; i < n; i++ )
      oper(fct, glob);
    logit( "call fct-obj oper(fct,glob)", t.elapsedNanos(), j );
  }

  FWKINFO( "CppPerf test run complete." );
  return 0;
}
