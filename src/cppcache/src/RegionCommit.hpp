/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * RegionCommit.hpp
 *
 *  Created on: 23-Feb-2011
 *      Author: ankurs
 */

#ifndef REGIONCOMMIT_HPP_
#define REGIONCOMMIT_HPP_

#include <gfcpp/gf_types.hpp>
#include <gfcpp/SharedBase.hpp>
#include <gfcpp/DataInput.hpp>
#include <gfcpp/VectorOfSharedBase.hpp>
#include <gfcpp/CacheableString.hpp>
#include <gfcpp/Cache.hpp>
#include <vector>
#include "FarSideEntryOp.hpp"

namespace gemfire {

_GF_PTR_DEF_(RegionCommit, RegionCommitPtr);

class RegionCommit : public gemfire::SharedBase {
 public:
  RegionCommit();
  virtual ~RegionCommit();

  void fromData(DataInput& input);
  void apply(Cache* cache);
  void fillEvents(Cache* cache, std::vector<FarSideEntryOpPtr>& ops);
  RegionPtr getRegion(Cache* cache) {
    return cache->getRegion(m_regionPath->asChar());
  }

 private:
  CacheableStringPtr m_regionPath;
  CacheableStringPtr m_parentRegionPath;
  VectorOfSharedBase m_farSideEntryOps;
};
}

#endif /* REGIONCOMMIT_HPP_ */
