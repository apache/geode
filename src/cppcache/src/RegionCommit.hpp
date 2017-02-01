#pragma once

#ifndef GEODE_REGIONCOMMIT_H_
#define GEODE_REGIONCOMMIT_H_

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
/*
 * RegionCommit.hpp
 *
 *  Created on: 23-Feb-2011
 *      Author: ankurs
 */


#include <gfcpp/gf_types.hpp>
#include <gfcpp/SharedBase.hpp>
#include <gfcpp/DataInput.hpp>
#include <gfcpp/VectorOfSharedBase.hpp>
#include <gfcpp/CacheableString.hpp>
#include <gfcpp/Cache.hpp>
#include <vector>
#include "FarSideEntryOp.hpp"

namespace apache {
namespace geode {
namespace client {

_GF_PTR_DEF_(RegionCommit, RegionCommitPtr);

class RegionCommit : public apache::geode::client::SharedBase {
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
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_REGIONCOMMIT_H_
