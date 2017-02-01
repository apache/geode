#pragma once

#ifndef APACHE_GEODE_GUARD_d8ed0294ced849b84153696edefc4a35
#define APACHE_GEODE_GUARD_d8ed0294ced849b84153696edefc4a35

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
 * @brief User class for testing the query functionality.
 */


#include <gfcpp/GeodeCppCache.hpp>
#include <string.h>
#include "fwklib/Timer.hpp"
#include "fwklib/FrameworkTest.hpp"
#include "TimestampedObject.hpp"
#include <ace/ACE.h>
#include <ace/OS.h>
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
class FastAsset;
typedef apache::geode::client::SharedPtr<FastAsset> FastAssetPtr;

class TESTOBJECT_EXPORT FastAsset : public TimestampedObject {
 private:
  int32_t assetId;
  double value;

  inline uint32_t getObjectSize(const SerializablePtr& obj) const {
    return (obj == NULLPTR ? 0 : obj->objectSize());
  }

 public:
  FastAsset() : assetId(0), value(0) {}
  FastAsset(int size, int maxVal);
  virtual ~FastAsset();
  virtual void toData(apache::geode::client::DataOutput& output) const;
  virtual apache::geode::client::Serializable* fromData(
      apache::geode::client::DataInput& input);
  virtual int32_t classId() const { return 24; }

  virtual uint32_t objectSize() const {
    uint32_t objectSize = sizeof(FastAsset);
    return objectSize;
  }

  /**
   * Returns the id of the asset.
   */
  int getAssetId() { return assetId; }

  /**
   * Returns the asset value.
   */
  double getValue() { return value; }

  /**
   * Sets the asset value.
   */
  void setValue(double d) { value = d; }

  int getIndex() { return assetId; }
  /**
   * Makes a copy of this asset.
   */
  FastAssetPtr copy() {
    FastAssetPtr asset(new FastAsset());
    asset->setAssetId(getAssetId());
    asset->setValue(getValue());
    return asset;
  }
  /**
   * Sets the id of the asset.
   */
  void setAssetId(int i) { assetId = i; }

  CacheableStringPtr toString() const {
    char buf[102500];
    sprintf(buf, "FastAsset:[assetId = %d value = %f]", assetId, value);
    return CacheableString::create(buf);
  }

  static apache::geode::client::Serializable* createDeserializable() {
    return new FastAsset();
  }
};

// typedef apache::geode::client::SharedPtr<FastAsset> FastAssetPtr;
}  // namespace testobject

#endif // APACHE_GEODE_GUARD_d8ed0294ced849b84153696edefc4a35
