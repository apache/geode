/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __ArrayOfByte_HPP__
#define __ArrayOfByte_HPP__

#include <gfcpp/GemfireCppCache.hpp>
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

using namespace gemfire;
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
        dos.writeInt((int32_t)index);
        if (encodeTimestamp) {
          ACE_Time_Value startTime;
          startTime = ACE_OS::gettimeofday();
          ACE_UINT64 tusec = 0;
          startTime.to_usec(tusec);
          int64_t timestamp = tusec * 1000;
          dos.writeInt((int64_t)timestamp);
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
      return CacheableBytes::createNoCopy((uint8_t *)buf, bufSize);
    } else if (encodeTimestamp) {
      FWKEXCEPTION("Should not happen");
    } else {
      return CacheableBytes::create(size);
    }
  }

  static int64_t getTimestamp(CacheableBytesPtr bytes) {
    if (bytes == NULLPTR) {
      throw gemfire::IllegalArgumentException("the bytes arg was null");
    }
    DataInput di(bytes->value(), bytes->length());
    try {
      int32_t index;
      di.readInt((int32_t *)&index);
      int64_t timestamp;
      di.readInt((int64_t *)&timestamp);
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
      di.readInt((int32_t *)&index);
      int64_t timestamp;
      di.readInt((int64_t *)&timestamp);
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
      dos.writeInt((int64_t)timestamp);
    } catch (Exception &e) {
      FWKEXCEPTION("Unable to write to stream " << e.getMessage());
    }
  }
};
}

#endif
