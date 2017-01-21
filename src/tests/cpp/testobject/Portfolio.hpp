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

#ifndef __PORTFOLIO_HPP__
#define __PORTFOLIO_HPP__

#include <gfcpp/GemfireCppCache.hpp>
#include "Position.hpp"

using namespace gemfire;

namespace testobject {

class TESTOBJECT_EXPORT Portfolio : public Serializable {
 private:
  int32_t ID;
  CacheableStringPtr pkid;
  PositionPtr position1;
  PositionPtr position2;
  CacheableHashMapPtr positions;
  CacheableStringPtr type;
  char* status;
  CacheableStringArrayPtr names;
  static const char* secIds[];
  uint8_t* newVal;
  int32_t newValSize;
  CacheableDatePtr creationDate;
  uint8_t* arrayNull;
  uint8_t* arrayZeroSize;

  inline uint32_t getObjectSize(const SerializablePtr& obj) const {
    return (obj == NULLPTR ? 0 : obj->objectSize());
  }

 public:
  Portfolio()
      : ID(0),
        pkid(NULLPTR),
        type(NULLPTR),
        status(NULL),
        newVal(NULL),
        creationDate(NULLPTR),
        arrayNull(NULL),
        arrayZeroSize(NULL) {}
  Portfolio(int32_t id, uint32_t size = 0,
            CacheableStringArrayPtr nm = NULLPTR);
  virtual ~Portfolio();

  virtual uint32_t objectSize() const {
    uint32_t objectSize = sizeof(Portfolio);
    objectSize += getObjectSize(pkid);
    objectSize += getObjectSize(position1);
    objectSize += getObjectSize(position2);
    objectSize += getObjectSize(positions);
    objectSize += getObjectSize(type);
    objectSize +=
        (status == NULL ? 0 : sizeof(char) * static_cast<uint32_t>(strlen(status)));
    objectSize += getObjectSize(names);
    objectSize += sizeof(uint8_t) * newValSize;
    objectSize += getObjectSize(creationDate);
    return objectSize;
  }

  int32_t getID() { return ID; }
  void showNames(const char* label) {
    LOGINFO(label);
    if (names == NULLPTR) {
      LOGINFO("names is NULL");
      return;
    }
    for (int i = 0; i < names->length(); i++) {
      LOGINFO("names[%d]=%s", i, names->operator[](i)->asChar());
    }
  }

  CacheableStringPtr getPkid() { return pkid; }

  PositionPtr getP1() { return position1; }

  PositionPtr getP2() { return position2; }

  CacheableHashMapPtr getPositions() { return positions; }

  bool testMethod(bool booleanArg) { return true; }

  char* getStatus() { return status; }

  bool isActive() { return (strcmp(status, "active") == 0) ? true : false; }

  uint8_t* getNewVal() { return newVal; }

  int32_t getNewValSize() { return newValSize; }

  CacheableStringPtr getType() { return this->type; }

  CacheableDatePtr getCreationDate() { return creationDate; }

  uint8_t* getArrayNull() { return arrayNull; }

  uint8_t* getArrayZeroSize() { return arrayZeroSize; }

  static Serializable* createDeserializable() { return new Portfolio(); }

  virtual void toData(DataOutput& output) const;
  virtual Serializable* fromData(DataInput& input);
  virtual int32_t classId() const { return 0x03; }
  CacheableStringPtr toString() const;
};

typedef SharedPtr<Portfolio> PortfolioPtr;
}  // namespace testobject
#endif
