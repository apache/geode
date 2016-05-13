/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
// Technical Report on C++ Performance ISO/IEC TR 18015:2006(E)
// © ISO/IEC 2006 — All rights reserved
// ----------------------------------------------------------------------------

#ifndef __CPPPERF_HPP__
#define __CPPPERF_HPP__

// ----------------------------------------------------------------------------

#include "fwklib/FrameworkTest.hpp"

namespace gemfire {
namespace testframework {
  
// ----------------------------------------------------------------------------

class CppPerf : public FrameworkTest
{
public:
  CppPerf( const char * initArgs ) :
    FrameworkTest( initArgs ) {}

    virtual ~CppPerf() {}

  void checkTest( const char * taskId );
  int32_t testCppPerf();

private:
};

} // namespace testframework
} // namespace gemfire

namespace ISO_IEC {
  
class X {
  int x;
  static int st;
public:
  virtual void f(int a);
  void g(int a);
  static void h(int a);
  void k(int i) { x+=i; } // inline
};

struct S {
  int x;
};

int glob = 0;

extern void f(S* p, int a);
extern void g(S* p, int a);
extern void h(int a);

typedef void (*PF)(S* p, int a);
PF p[10] = { g , f };

#define K(p,i) ((p)->x+=(i))

using namespace gemfire;

struct T {
  const char* s;
  int64_t t;
  T(const char* ss, int64_t tt) : s(ss), t(tt) {}
  T() : s(0), t(0) {}
};

struct A {
  int x;
  virtual void f(int) = 0;
  void g(int);
};

struct B {
  int xx;
  virtual void ff(int) = 0;
  void gg(int);
};

struct C : A, B {
  void f(int);
  void ff(int);
};

struct CC : A, B {
  void f(int);
  void ff(int);
};

void A::g(int i) { x += i; }
void B::gg(int i) { xx += i; }
void C::f(int i) { x += i; }
void C::ff(int i) { xx += i; }
void CC::f(int i) { x += i; }
void CC::ff(int i) { xx += i; }

template<class T, class T2> inline T* cast(T* p, T2* q)
{
  glob++;
  return dynamic_cast<T*>(q);
}

struct C2 : virtual A { // note: virtual base
};

struct C3 : virtual A {
};

struct D : C2, C3 { // note: virtual base
  void f(int);
};

void D::f(int i) { x+=i; }
struct P {
  int x;
  int y;
};

void by_ref(P& a) { a.x++; a.y++; }
void by_val(P a) { a.x++; a.y++; }

template<class F, class V> inline void oper(F f, V val) { f(val); }

struct FO {
  void operator () (int i) { glob += i; }
};

} // namespace ISO_IEC

#endif // __CPPPERF_HPP__
