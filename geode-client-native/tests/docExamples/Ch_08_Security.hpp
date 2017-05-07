/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef SECURITY_HPP_
#define SECURITY_HPP_

/*
* This is the example to verify the code snippets given in the native client guide chapter 8.
* This is the example with Security.
*/

#include <gfcpp/gf_types.hpp>
#include "gfcpp/GemfireCppCache.hpp"

using namespace gemfire;

class Security
{
public:
  Security();
  ~Security();
  void startServer();
  void stopServer();
  void example_8_1();
};

#endif /* SECURITY_HPP_ */
