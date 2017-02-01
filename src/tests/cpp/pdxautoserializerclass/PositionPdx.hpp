#pragma once

#ifndef APACHE_GEODE_GUARD_fc0317ed5799f03f58df423db009e0b5
#define APACHE_GEODE_GUARD_fc0317ed5799f03f58df423db009e0b5

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


#include <gfcpp/GeodeCppCache.hpp>
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
#define GFIGNORE(X) X
#define GFEXCLUDE
#define GFID
#define GFARRAYSIZE(X)
#define GFARRAYELEMSIZE(X)

using namespace apache::geode::client;

namespace AutoPdxTests {

class GFIGNORE(TESTOBJECT_EXPORT) PositionPdx
    : public apache::geode::client::PdxSerializable {
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
  GFID char *secId, *secLinks, *secType;
  GFID int32_t sharesOutstanding;
  GFID char* underlyer;
  GFID int64_t volatility;

  GFID int32_t pid;

 public:
  GFEXCLUDE static int32_t cnt;

  PositionPdx();
  PositionPdx(const char* id, int32_t out);
  // This constructor is just for some internal data validation test
  PositionPdx(int32_t iForExactVal);
  virtual ~PositionPdx();
  using PdxSerializable::toData;
  using PdxSerializable::fromData;
  virtual void toData(PdxWriterPtr pw);
  virtual void fromData(PdxReaderPtr pr);

  CacheableStringPtr toString() const;

  virtual uint32_t objectSize() const {
    uint32_t objectSize = sizeof(PositionPdx);
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

typedef apache::geode::client::SharedPtr<PositionPdx> PositionPdxPtr;
}  // namespace AutoPdxTests

#endif // APACHE_GEODE_GUARD_fc0317ed5799f03f58df423db009e0b5
