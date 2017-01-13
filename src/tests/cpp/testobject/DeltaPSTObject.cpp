/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "fwklib/GsRandom.hpp"
#include "ArrayOfByte.hpp"
#include "DeltaPSTObject.hpp"

using namespace gemfire;
using namespace testframework;
using namespace testobject;

DeltaPSTObject::DeltaPSTObject(int size, bool encodeKey, bool encodeTimestamp) {
  ACE_Time_Value startTime;
  startTime = ACE_OS::gettimeofday();
  ACE_UINT64 tusec = 0;
  startTime.to_usec(tusec);
  timestamp = tusec * 1000;
  field1 = 1234;
  field2 = '*';
  if (size == 0) {
    valueData = NULLPTR;
  } else {
    encodeKey = true;
    valueData = ArrayOfByte::init(size, encodeKey, false);
  }
}
void DeltaPSTObject::fromDelta(DataInput& input) {
  input.readInt(&field1);
  input.readInt(reinterpret_cast<int64_t*>(&timestamp));
}
void DeltaPSTObject::toDelta(DataOutput& output) const {
  output.writeInt(static_cast<int32_t>(field1));
  output.writeInt(static_cast<int64_t>(timestamp));
}
void DeltaPSTObject::toData(gemfire::DataOutput& output) const {
  output.writeInt(static_cast<int64_t>(timestamp));
  output.writeInt(static_cast<int32_t>(field1));
  output.write(field2);
  output.writeObject(valueData);
}

gemfire::Serializable* DeltaPSTObject::fromData(gemfire::DataInput& input) {
  input.readInt(reinterpret_cast<int64_t*>(&timestamp));
  input.readInt(&field1);
  input.read(&field2);
  input.readObject(valueData);
  return this;
}
CacheableStringPtr DeltaPSTObject::toString() const {
  char buf[102500];
  sprintf(
      buf,
      "DeltaPSTObject:[timestamp = %lld field1 = %d field2 = %c valueData=%d ]",
      timestamp, field1, field2, valueData->length());
  return CacheableString::create(buf);
}
