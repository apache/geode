/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _GEMFIRE_GEMFIRETYPEIDS_HPP_
#define _GEMFIRE_GEMFIRETYPEIDS_HPP_

namespace gemfire {

class GemfireTypeIds {
 public:
  // User visible IDs here
  // Internal IDs are in GemfireTypeIdsImpl.hpp

  enum IdValues {
    // Do not use IDs 5 and 6 which are used by .NET
    // ManagedObject and ManagedObjectXml. If those are
    // required then change those in GemfireTypeIdsM.hpp

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
}

#endif
