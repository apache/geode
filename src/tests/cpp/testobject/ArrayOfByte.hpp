#pragma once

#ifndef APACHE_GEODE_GUARD_ec3211e9438cbef839aaab90eac09a4b
#define APACHE_GEODE_GUARD_ec3211e9438cbef839aaab90eac09a4b

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

#include <gfcpp/GeodeCppCache.hpp>
#include <string.h>
#include "fwklib/FwkLog.hpp"
#include "fwklib/FrameworkTest.hpp"
#include <ace/Time_Value.h>

#ifdef _WIN32
#ifdef BUILD_TESTOBJECT
#define TESTOBJECT_EXPORT LIBEXP
#else
#define TESTOBJECT_EXPORT LIBIMP
#endif
#else
#define TESTOBJECT_EXPORT
#endif

using namespace apache::geode::client;
using namespace testframework;

namespace testobject {

class TESTOBJECT_EXPORT ArrayOfByte {
 public:
  static CacheableBytesPtr init(int size, bool encodeKey,
                                bool encodeTimestamp) {
    if (encodeKey) {
      DataOutput dos;
      try {
        int32_t index = 1234;
        dos.writeInt(index);
        if (encodeTimestamp) {
          ACE_Time_Value startTime;
          startTime = ACE_OS::gettimeofday();
          ACE_UINT64 tusec = 0;
          startTime.to_usec(tusec);
          int64_t timestamp = tusec * 1000;
          dos.writeInt(timestamp);
        }
      } catch (Exception &e) {
        FWKEXCEPTION("Unable to write to stream " << e.getMessage());
      }
      int32 bufSize = size;
      char *buf = new char[bufSize];
      memset(buf, 'V', bufSize);
      int32 rsiz = (bufSize <= 20) ? bufSize : 20;
      GsRandom::getAlphanumericString(rsiz, buf);
      memcpy(buf, dos.getBuffer(), dos.getBufferLength());
      return CacheableBytes::createNoCopy(reinterpret_cast<uint8_t *>(buf),
                                          bufSize);
    } else if (encodeTimestamp) {
      FWKEXCEPTION("Should not happen");
    } else {
      return CacheableBytes::create(size);
    }
  }

  static int64_t getTimestamp(CacheableBytesPtr bytes) {
    if (bytes == NULLPTR) {
      throw apache::geode::client::IllegalArgumentException(
          "the bytes arg was null");
    }
    DataInput di(bytes->value(), bytes->length());
    try {
      int32_t index;
      di.readInt(&index);
      int64_t timestamp;
      di.readInt(&timestamp);
      if (timestamp == 0) {
        FWKEXCEPTION("Object is not configured to encode timestamp");
      }
      return timestamp;
    } catch (Exception &e) {
      FWKEXCEPTION("Unable to read from stream " << e.getMessage());
    }
  }

  static void resetTimestamp(CacheableBytesPtr bytes) {
    DataInput di(bytes->value(), bytes->length());
    int32_t index;
    try {
      di.readInt(&index);
      int64_t timestamp;
      di.readInt(&timestamp);
      if (timestamp == 0) {
        return;
      }
    } catch (Exception &e) {
      FWKEXCEPTION("Unable to read from stream " << e.getMessage());
    }
    DataOutput dos;
    try {
      dos.writeInt(index);
      ACE_Time_Value startTime;
      startTime = ACE_OS::gettimeofday();
      ACE_UINT64 tusec = 0;
      startTime.to_usec(tusec);
      int64_t timestamp = tusec * 1000;
      dos.writeInt(timestamp);
    } catch (Exception &e) {
      FWKEXCEPTION("Unable to write to stream " << e.getMessage());
    }
  }
};
}  // namespace testobject


#endif // APACHE_GEODE_GUARD_ec3211e9438cbef839aaab90eac09a4b
