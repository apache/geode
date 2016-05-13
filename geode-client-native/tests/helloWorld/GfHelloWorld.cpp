/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#if 0
#include <ace/Select_Reactor.h>

int main(int argc, char **argv) {
  ACE_Select_Reactor* asr = new ACE_Select_Reactor();
  return 0;
}
#endif
#if 1

#include <gfcpp/GemfireCppCache.hpp>

using namespace gemfire;

/*
  This example print hello world, testing libary loading
*/
int main( int argc, char** argv )
{
  RegionPtr rptr;
  printf("hello world!\n");
  
  return 0;
}
#endif
