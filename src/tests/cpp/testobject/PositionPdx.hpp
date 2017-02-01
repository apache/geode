#pragma once

#ifndef APACHE_GEODE_GUARD_16153fc06e90eeaa167430a34632e57d
#define APACHE_GEODE_GUARD_16153fc06e90eeaa167430a34632e57d

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

using namespace apache::geode::client;

namespace testobject {

class TESTOBJECT_EXPORT PositionPdx
    : public apache::geode::client::PdxSerializable {
 private:
  int64_t avg20DaysVol;
  char* bondRating;
  double convRatio;
  char* country;
  double delta;
  int64_t industry;
  int64_t issuer;
  double mktValue;
  double qty;
  char* secId;
  char* secLinks;
  // wchar_t* secType;
  // wchar_t* secType;
  char* secType;
  int32_t sharesOutstanding;
  char* underlyer;
  int64_t volatility;

  int32_t pid;

 public:
  static int32_t cnt;

  PositionPdx();
  PositionPdx(const char* id, int32_t out);
  // This constructor is just for some internal data validation test
  PositionPdx(int32_t iForExactVal);
  virtual ~PositionPdx();
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

  static PdxSerializable* createDeserializable() { return new PositionPdx(); }

  const char* getClassName() const { return "testobject.PositionPdx"; }

 private:
  void init();
};

typedef apache::geode::client::SharedPtr<PositionPdx> PositionPdxPtr;
}  // namespace testobject

#endif // APACHE_GEODE_GUARD_16153fc06e90eeaa167430a34632e57d
