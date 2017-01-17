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
#include "PSTObject.hpp"
#include "fwklib/GsRandom.hpp"
#include "ArrayOfByte.hpp"

using namespace gemfire;
using namespace testframework;
using namespace testobject;

PSTObject::PSTObject(int size, bool encodeKey, bool encodeTimestamp) {
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

PSTObject::~PSTObject() {}
void PSTObject::toData(gemfire::DataOutput& output) const {
  output.writeInt(static_cast<int64_t>(timestamp));
  output.writeInt(static_cast<int32_t>(field1));
  output.write(field2);
  output.writeObject(valueData);
}

gemfire::Serializable* PSTObject::fromData(gemfire::DataInput& input) {
  input.readInt(reinterpret_cast<int64_t*>(&timestamp));
  input.readInt(&field1);
  input.read(&field2);
  input.readObject(valueData);
  return this;
}
CacheableStringPtr PSTObject::toString() const {
  char buf[102500];
  sprintf(buf,
          "PSTObject:[timestamp = %lld field1 = %d field2 = %c valueData=%d ]",
          timestamp, field1, field2, valueData->length());
  return CacheableString::create(buf);
}
