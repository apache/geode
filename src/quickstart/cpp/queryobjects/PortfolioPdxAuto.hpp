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

#ifndef __PORTFOLIOPDX_AUTO_HPP__
#define __PORTFOLIOPDX_AUTO_HPP__

#include "PositionPdxAuto.hpp"

using namespace gemfire;

namespace testobject {
#define GFARRAYSIZE(X)
#define GFID
class PortfolioPdxAuto : public gemfire::PdxSerializable {
 private:
  GFID int32_t id;

  GFID char* pkid;

  GFID PositionPdxPtr position1;
  GFID PositionPdxPtr position2;
  GFID CacheableHashMapPtr positions;
  GFID char* type;
  GFID char* status;
  // char** names;
  // static const char* secIds[];
  GFID int8_t* newVal;
  GFARRAYSIZE(newVal) int32_t newValSize;
  GFID CacheableDatePtr creationDate;
  GFID int8_t* arrayNull;
  GFARRAYSIZE(arrayNull) int32_t arrayNullSize;

  GFID int8_t* arrayZeroSize;
  GFARRAYSIZE(arrayZeroSize) int32_t arrayZeroSizeSize;

 public:
  PortfolioPdxAuto()
      : id(0),
        pkid(NULL),
        type(NULL),
        status(NULL),
        newVal(NULL),
        creationDate(NULLPTR),
        arrayNull(NULL),
        arrayNullSize(0),
        arrayZeroSize(NULL),
        arrayZeroSizeSize(0) {}

  PortfolioPdxAuto(int32_t id, int32_t size = 0, char** nm = NULL);

  virtual ~PortfolioPdxAuto();

  int32_t getID() { return id; }

  char* getPkid() { return pkid; }

  PositionPdxPtr getP1() { return position1; }

  PositionPdxPtr getP2() { return position2; }

  CacheableHashMapPtr getPositions() { return positions; }

  bool testMethod(bool booleanArg) { return true; }

  char* getStatus() { return status; }

  bool isActive() { return (strcmp(status, "active") == 0) ? true : false; }

  int8_t* getNewVal() { return newVal; }

  int32_t getNewValSize() { return newValSize; }

  const char* getClassName() { return this->type; }

  CacheableDatePtr getCreationDate() { return creationDate; }

  int8_t* getArrayNull() { return arrayNull; }

  int8_t* getArrayZeroSize() { return arrayZeroSize; }

  const char* getClassName() const;

  virtual void toData(PdxWriterPtr pw);
  virtual void fromData(PdxReaderPtr pr);

  static PdxSerializable* createDeserializable();

  CacheableStringPtr toString() const;
};

typedef SharedPtr<PortfolioPdxAuto> PortfolioPdxPtr;
}
#endif
