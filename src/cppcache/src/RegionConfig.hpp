/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

namespace apache {
namespace geode {
namespace client {

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
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif  // !defined(AFX_CACHEREGION_H__B8A44D1C_F9A4_49A8_A3A2_86CAE5E73C6F__INCLUDED_)
