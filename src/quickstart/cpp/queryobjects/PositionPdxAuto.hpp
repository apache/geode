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

/*
 * @brief User class for testing the put functionality for object.
 */

#ifndef __POSITIONPDX_AUTO__HPP__
#define __POSITIONPDX_AUTO__HPP__

#include <gfcpp/GemfireCppCache.hpp>
#include <gfcpp/PdxSerializable.hpp>
#include <gfcpp/PdxWriter.hpp>
#include <gfcpp/PdxReader.hpp>
#include <string.h>

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

namespace testobject {

#define GFID

class PositionPdxAuto : public gemfire::PdxSerializable {
 private:
  GFID int64_t avg20DaysVol;
  GFID char* bondRating;
  GFID double convRatio;
  GFID char* country;
  GFID double delta;
  GFID int64_t industry;
  GFID int64_t issuer;
  GFID double mktValue;
  GFID double qty;
  GFID char* secId;
  GFID char* secLinks;
  // wchar_t* secType;
  // wchar_t* secType;
  GFID char* secType;
  GFID int32_t sharesOutstanding;
  GFID char* underlyer;
  GFID int64_t volatility;

  GFID int32_t pid;

 public:
  static int32_t cnt;

  PositionPdxAuto();
  PositionPdxAuto(const char* id, int32_t out);
  // This constructor is just for some internal data validation test
  PositionPdxAuto(int32_t iForExactVal);
  virtual ~PositionPdxAuto();
  virtual void toData(PdxWriterPtr pw);
  virtual void fromData(PdxReaderPtr pr);

  CacheableStringPtr toString() const;

  virtual uint32_t objectSize() const {
    uint32_t objectSize = sizeof(PositionPdxAuto);
    return objectSize;
  }

  static void resetCounter() { cnt = 0; }

  char* getSecId() { return secId; }

  int32_t getId() { return pid; }

  int32_t getSharesOutstanding() { return sharesOutstanding; }

  static PdxSerializable* createDeserializable();

  const char* getClassName() const;

 private:
  void init();
};

typedef gemfire::SharedPtr<PositionPdxAuto> PositionPdxPtr;
}
#endif
