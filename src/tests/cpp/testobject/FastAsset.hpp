/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/*
 * @brief User class for testing the query functionality.
 */

#ifndef __FASTASSET__HPP__
#define __FASTASSET__HPP__

#include <gfcpp/GemfireCppCache.hpp>
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

using namespace gemfire;
using namespace testframework;
namespace testobject {
class FastAsset;
typedef gemfire::SharedPtr<FastAsset> FastAssetPtr;

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
  virtual void toData(gemfire::DataOutput& output) const;
  virtual gemfire::Serializable* fromData(gemfire::DataInput& input);
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

  static gemfire::Serializable* createDeserializable() {
    return new FastAsset();
  }
};

// typedef gemfire::SharedPtr<FastAsset> FastAssetPtr;
}
#endif
