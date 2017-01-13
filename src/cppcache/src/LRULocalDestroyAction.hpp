/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __GEMFIRE_IMPL_LRULOCALDESTROYACTION_HPP__
#define __GEMFIRE_IMPL_LRULOCALDESTROYACTION_HPP__

#include <gfcpp/gfcpp_globals.hpp>
#include "LRUAction.hpp"
#include "RegionInternal.hpp"
#include "MapEntry.hpp"

namespace gemfire {

/**
 * @brief LRUAction for localDestroy.
 */
class CPPCACHE_EXPORT LRULocalDestroyAction : public virtual LRUAction {
 private:
  RegionInternal* m_regionPtr;
  LRUEntriesMap* m_entriesMapPtr;

  LRULocalDestroyAction(RegionInternal* regionPtr, LRUEntriesMap* entriesMapPtr)
      : m_regionPtr(regionPtr), m_entriesMapPtr(entriesMapPtr) {
    m_destroys = true;
  }

 public:
  virtual ~LRULocalDestroyAction() {}

  virtual bool evict(const MapEntryImplPtr& mePtr);

  virtual LRUAction::Action getType() { return LRUAction::LOCAL_DESTROY; }
  friend class LRUAction;
};
};

#endif  //__GEMFIRE_IMPL_LRULOCALDESTROYACTION_HPP__
