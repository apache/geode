#ifndef __GEMFIRE_REGIONGLOBAL_H__
#define __GEMFIRE_REGIONGLOBAL_H__

/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*
* The specification of function behaviors is found in the corresponding .cpp
*file.
*
*
*========================================================================
*/

#include "LocalRegion.hpp"

namespace gemfire {

class CPPCACHE_EXPORT RegionGlobalLocks {
 public:
  RegionGlobalLocks(LocalRegion* region, bool isFailover = true)
      : m_region(region), m_isFailover(isFailover) {
    m_region->acquireGlobals(m_isFailover);
  }

  ~RegionGlobalLocks() { m_region->releaseGlobals(m_isFailover); }

 private:
  LocalRegion* m_region;
  bool m_isFailover;
};
};  // namespace gemfire

#endif  // ifndef __GEMFIRE_REGIONGLOBAL_H__
