/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef SETTINGPROPERTIES_HPP_
#define SETTINGPROPERTIES_HPP_

/*
* This is the example to verify the code snippets given in the native client guide chapter 6.
* This is the example with Setting Properties.
*/

#include <gfcpp/gf_types.hpp>
#include "gfcpp/GemfireCppCache.hpp"

using namespace gemfire;

class SettingProperties
{
public:
  SettingProperties();
  ~SettingProperties();
  void startServer();
  void stopServer();
  void example_6_1();
};

#endif /* SETTINGPROPERTIES_HPP_ */
