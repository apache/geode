/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

/**
 * @file
 */
// cacheRegion.h: interface for the cacheRegion class.
//
//////////////////////////////////////////////////////////////////////

#if !defined(AFX_CACHEREGION_H__B8A44D1C_F9A4_49A8_A3A2_86CAE5E73C6F__INCLUDED_)
#define AFX_CACHEREGION_H__B8A44D1C_F9A4_49A8_A3A2_86CAE5E73C6F__INCLUDED_

#if _MSC_VER > 1000
#pragma once
#endif  // _MSC_VER > 1000
#include <gfcpp/SharedPtr.hpp>

#include <string>
#include <gfcpp/Properties.hpp>
#include <stdlib.h>

namespace gemfire {

class RegionConfig;
typedef SharedPtr<RegionConfig> RegionConfigPtr;

class CPPCACHE_EXPORT RegionConfig : virtual public SharedBase {
 public:
  RegionConfig(const std::string& s, const std::string& c);

  unsigned long entries();
  void setConcurrency(const std::string& str);
  void setLru(const std::string& str);
  void setCaching(const std::string& str);
  uint8_t getConcurrency();
  unsigned long getLruEntriesLimit();
  bool getCaching();

 private:
  std::string m_capacity;
  std::string m_lruEntriesLimit;
  std::string m_concurrency;
  std::string m_caching;
};
};

#endif  // !defined(AFX_CACHEREGION_H__B8A44D1C_F9A4_49A8_A3A2_86CAE5E73C6F__INCLUDED_)
