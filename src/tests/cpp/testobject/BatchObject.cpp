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
