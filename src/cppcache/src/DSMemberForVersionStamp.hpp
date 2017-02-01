#pragma once

#ifndef GEODE_DSMEMBERFORVERSIONSTAMP_H_
#define GEODE_DSMEMBERFORVERSIONSTAMP_H_

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

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/CacheableKey.hpp>
#include <string>

namespace apache {
namespace geode {
namespace client {
class DSMemberForVersionStamp;
typedef SharedPtr<DSMemberForVersionStamp> DSMemberForVersionStampPtr;

class DSMemberForVersionStamp : public CacheableKey {
 public:
  virtual int16_t compareTo(DSMemberForVersionStampPtr tagID) = 0;

  virtual std::string getHashKey() = 0;

  /** return true if this key matches other. */
  virtual bool operator==(const CacheableKey& other) const = 0;

  /** return the hashcode for this key. */
  virtual uint32_t hashcode() const = 0;
};
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_DSMEMBERFORVERSIONSTAMP_H_
