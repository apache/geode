/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "BatchObject.hpp"

using namespace gemfire;
using namespace testframework;
using namespace testobject;

BatchObject::BatchObject(int32_t anIndex, int32_t batchSize, int32_t size) {
  index = anIndex;
  ACE_Time_Value startTime;
  startTime = ACE_OS::gettimeofday();
  ACE_UINT64 tusec = 0;
  startTime.to_usec(tusec);
  timestamp = tusec * 1000;
  batch = anIndex / batchSize;
  byteArray = CacheableBytes::create(size);
}

BatchObject::~BatchObject() {}
void BatchObject::toData(gemfire::DataOutput& output) const {
  output.writeInt(static_cast<int32_t>(index));
  output.writeInt(static_cast<int64_t>(timestamp));
  output.writeInt(static_cast<int32_t>(batch));
  output.writeObject(byteArray);
}

gemfire::Serializable* BatchObject::fromData(gemfire::DataInput& input) {
  input.readInt(&index);
  input.readInt(reinterpret_cast<int64_t*>(&timestamp));
  input.readInt(&batch);
  input.readObject(byteArray);
  return this;
}
CacheableStringPtr BatchObject::toString() const {
  char buf[102500];
  sprintf(buf,
          "BatchObject:[index = %d timestamp = %lld batch = %d byteArray=%d ]",
          index, timestamp, batch, byteArray->length());
  return CacheableString::create(buf);
}
