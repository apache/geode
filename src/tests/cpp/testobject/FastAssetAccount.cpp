/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "FastAssetAccount.hpp"
#include "FastAsset.hpp"

using namespace gemfire;
using namespace testframework;
using namespace testobject;

FastAssetAccount::FastAssetAccount(int idx, bool encodeTimestp, int maxVal,
                                   int asstSize)
    : encodeTimestamp(encodeTimestp), acctId(idx) {
  // encodeTimestamp = encodeTimestp;
  // acctId = idx;
  customerName = CacheableString::create("Milton Moneybags");
  netWorth = 0.0;
  assets = CacheableHashMap::create();
  for (int i = 0; i < asstSize; i++) {
    FastAssetPtr asset(new FastAsset(i, maxVal));
    assets->insert(CacheableInt32::create(i), asset);
    netWorth += asset->getValue();
  }
  if (encodeTimestamp) {
    ACE_Time_Value startTime;
    startTime = ACE_OS::gettimeofday();
    ACE_UINT64 tusec = 0;
    startTime.to_usec(tusec);
    timestamp = tusec * 1000;
  }
}

FastAssetAccount::~FastAssetAccount() {}
void FastAssetAccount::toData(gemfire::DataOutput& output) const {
  // output.writeBoolean(encodeTimestamp);
  output.writeInt(static_cast<int32_t>(acctId));
  output.writeObject(customerName);
  output.writeDouble(netWorth);
  output.writeObject(assets);
  output.writeInt(static_cast<int64_t>(timestamp));
}

gemfire::Serializable* FastAssetAccount::fromData(gemfire::DataInput& input) {
  // input.readBoolean(&encodeTimestamp);
  input.readInt(&acctId);
  input.readObject(customerName);
  input.readDouble(&netWorth);
  input.readObject(assets);
  input.readInt(reinterpret_cast<int64_t*>(&timestamp));
  return this;
}
CacheableStringPtr FastAssetAccount::toString() const {
  char buf[102500];
  sprintf(buf,
          "FastAssetAccount:[acctId = %d customerName = %s netWorth = %f "
          "timestamp = %" PRIu64 "]",
          acctId, customerName->toString(), netWorth, timestamp);
  return CacheableString::create(buf);
}
