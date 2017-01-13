/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _ADMIN_REGION_HPP_INCLUDED_
#define _ADMIN_REGION_HPP_INCLUDED_

#include <gfcpp/gf_types.hpp>
#include "ThinClientCacheDistributionManager.hpp"
#include "ReadWriteLock.hpp"
#include <gfcpp/Serializable.hpp>
#include <gfcpp/SharedPtr.hpp>
//#include <statistics/HostStatSampler.hpp>

#include "NonCopyable.hpp"
namespace gemfire_statistics {
class HostStatSampler;
}

namespace gemfire {
class CacheImpl;
/* adongre
 * CID 28724: Other violation (MISSING_COPY)
 * Class "gemfire::AdminRegion" owns resources that are managed in its
 * constructor and destructor but has no user-written copy constructor.
 *
 * CID 28710: Other violation (MISSING_ASSIGN)
 * Class "gemfire::AdminRegion" owns resources that are managed in its
 * constructor and destructor but has no user-written assignment operator.
 *
 * FIX : Make the class noncopyabl3
 */
class AdminRegion : public SharedBase,
                    private NonCopyable,
                    private NonAssignable {
 private:
  ThinClientBaseDM* m_distMngr;
  std::string m_fullPath;
  TcrConnectionManager* m_connectionMgr;
  ACE_RW_Thread_Mutex m_rwLock;
  bool m_destroyPending;

  GfErrType putNoThrow(const CacheableKeyPtr& keyPtr,
                       const CacheablePtr& valuePtr);
  TcrConnectionManager* getConnectionManager();

 public:
  ACE_RW_Thread_Mutex& getRWLock();
  const bool& isDestroyed();
  void close();
  void init();
  void put(const CacheableKeyPtr& keyPtr, const CacheablePtr& valuePtr);
  AdminRegion(CacheImpl* cache, ThinClientBaseDM* distMan = NULL);
  ~AdminRegion();
  friend class gemfire_statistics::HostStatSampler;
};

typedef SharedPtr<AdminRegion> AdminRegionPtr;
}  // namespace gemfire

#endif
