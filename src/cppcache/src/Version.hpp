/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __GEMFIRE_VERSION_HPP__
#define __GEMFIRE_VERSION_HPP__
#include "CacheImpl.hpp"

namespace gemfire {

class Version {
 public:
  // getter for ordinal
  static int8_t getOrdinal() { return Version::m_ordinal; }

  friend void gemfire::CacheImpl::setVersionOrdinalForTest(int8_t newOrdinal);
  friend int8_t gemfire::CacheImpl::getVersionOrdinalForTest();

 private:
  static int8_t m_ordinal;

  Version(){};
};

}  // namespace gemfire

#endif  //__GEMFIRE_VERSION_HPP__s
