#pragma once

#ifndef GEODE_GFCPP_GEODETYPEIDS_H_
#define GEODE_GFCPP_GEODETYPEIDS_H_

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

namespace apache {
namespace geode {
namespace client {

class GeodeTypeIds {
 public:
  // User visible IDs here
  // Internal IDs are in GeodeTypeIds.hpp

  enum IdValues {
    // Do not use IDs 5 and 6 which are used by .NET
    // ManagedObject and ManagedObjectXml. If those are
    // required then change those in GeodeTypeIdsM.hpp

    // keep the following in alphabetical order please.
    EnumInfo = 9,
    CacheableLinkedList = 10,
    Properties = 11,
    PdxType = 17,  // internal hack to read pdxtype in c# layer, look usuage in
                   // TcrMessage and  C# DistributedM.cpp
    BooleanArray = 26,
    CharArray = 27,
    RegionAttributes = 30,  // because there's no equivalence in java
    CacheableUndefined = 31,
    Struct = 32,
    NullObj = 41,
    CacheableString = 42,
    CacheableBytes = 46,
    CacheableInt16Array = 47,
    CacheableInt32Array = 48,
    CacheableInt64Array = 49,
    CacheableFloatArray = 50,
    CacheableDoubleArray = 51,
    CacheableObjectArray = 52,
    CacheableBoolean = 53,
    CacheableWideChar = 54,
    CacheableByte = 55,
    CacheableInt16 = 56,
    CacheableInt32 = 57,
    CacheableInt64 = 58,
    CacheableFloat = 59,
    CacheableDouble = 60,
    CacheableDate = 61,
    CacheableFileName = 63,
    CacheableStringArray = 64,
    CacheableArrayList = 65,
    CacheableHashSet = 66,
    CacheableHashMap = 67,
    CacheableTimeUnit = 68,
    CacheableNullString = 69,
    CacheableHashTable = 70,
    CacheableVector = 71,
    CacheableIdentityHashMap = 72,
    CacheableLinkedHashSet = 73,
    CacheableStack = 74,
    CacheableASCIIString = 87,
    CacheableASCIIStringHuge = 88,
    CacheableStringHuge = 89,
    CacheableEnum = 94
  };
};
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_GFCPP_GEODETYPEIDS_H_
