/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "DeltaFastAssetAccount.hpp"

using namespace gemfire;
using namespace testframework;
using namespace testobject;

DeltaFastAssetAccount::DeltaFastAssetAccount(int index, bool encodeTimestp,
                                             int maxVal, int asstSize,
                                             bool getbfrUpdate)
    : encodeTimestamp(encodeTimestp), acctId(index) {
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
  getBeforeUpdate = getbfrUpdate;
}

void DeltaFastAssetAccount::toData(gemfire::DataOutput& output) const {
  output.writeInt(static_cast<int32_t>(acctId));
  output.writeObject(customerName);
  output.writeDouble(netWorth);
  output.writeObject(assets);
  output.writeInt(static_cast<int64_t>(timestamp));
}

gemfire::Serializable* DeltaFastAssetAccount::fromData(
    gemfire::DataInput& input) {
  input.readInt(&acctId);
  input.readObject(customerName);
  input.readDouble(&netWorth);
  input.readObject(assets);
  input.readInt(reinterpret_cast<int64_t*>(&timestamp));
  return this;
}

void DeltaFastAssetAccount::toDelta(gemfire::DataOutput& output) const {
  output.writeDouble(netWorth);
  if (encodeTimestamp) {
    output.writeInt(static_cast<int64_t>(timestamp));
  }
}
void DeltaFastAssetAccount::fromDelta(gemfire::DataInput& input) {
  if (getBeforeUpdate) {
    input.readDouble(&netWorth);
  } else {
    double netWorthTemp;
    input.readDouble(&netWorthTemp);
    netWorth += netWorthTemp;
  }
  if (encodeTimestamp) {
    input.readInt(reinterpret_cast<int64_t*>(&timestamp));
  }
}
