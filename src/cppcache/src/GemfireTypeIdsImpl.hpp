/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _GEMFIRE_GEMFIRETYPEIDSIMPL_HPP_
#define _GEMFIRE_GEMFIRETYPEIDSIMPL_HPP_

namespace gemfire {

class GemfireTypeIdsImpl {
 public:
  // Internal IDs here
  // User visible IDs are in GemfireTypeIds.hpp

  enum IdValues {
    // keep the following in alphabetical order please.
    ObjectTypeImpl = -61,
    StructTypeImpl = -60,
    CollectionTypeImpl = -59,
    FixedIDDefault = 0,
    FixedIDByte = 1,
    FixedIDShort = 2,
    FixedIDInt = 3,
    FixedIDNone = 4,
    CacheableToken = 14,  // because there's no equivalence in java
    VersionedObjectPartList = 7,
    CacheableObjectPartList = 25,
    EventId = 36,
    InterestResultPolicy = 37,
    ClientProxyMembershipId = 38,
    CacheableUserData4 = 37,
    CacheableUserData2 = 38,
    CacheableUserData = 39,
    CacheableUserClass = 40,
    Class = 43,
    JavaSerializable = 44,
    DataSerializable = 45,
    InternalDistributedMember = 92,
    PDX = 93,
    // PDX_ENUM = 94,
    EntryEventImpl = 105,
    RegionEventImpl = 108,
    ClientHealthStats = -126,
    GatewayEventCallbackArgument = -56,         // 0xC8
    GatewaySenderEventCallbackArgument = -135,  // 0xC8
    ClientConnectionRequest = -53,
    ClientConnectionResponse = -50,
    QueueConnectionRequest = -52,
    QueueConnectionResponse = -49,
    LocatorListRequest = -54,
    LocatorListResponse = -51,
    GetAllServersRequest = -43,
    GetAllServersResponse = -42,
    ClientReplacementRequest = -48,
    VmCachedDeserializable = -64,
    PreferBytesCachedDeserializable = -65,
    TXCommitMessage = 110,
    CacheableObjectPartList66 = 2121,
    VersionTag = -120,
    DiskStoreId = 2133,
    DiskVersionTag = 2131
  };
};
}

#endif
