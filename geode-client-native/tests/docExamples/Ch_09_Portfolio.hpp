/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef CH_09_PORTFOLIO_HPP_
#define CH_09_PORTFOLIO_HPP_


#include <gfcpp/GemfireCppCache.hpp>

using namespace gemfire;

class Position;

/**
* @brief Example 9.1 C++ Class Definition.
*/
class Ch_09_Portfolio : public Serializable {
  int ID;
  char * type;
  char * status;
  CacheableHashMapPtr positions;
};

class Position : public Serializable {
  char * secId;
  double mktValue;
  double qty;
};

#endif /* CH_09_PORTFOLIO_HPP_ */
