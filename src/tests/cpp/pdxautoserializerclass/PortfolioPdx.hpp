#pragma once

#ifndef APACHE_GEODE_GUARD_115ad9fa08f9b8368045cfda498ff99b
#define APACHE_GEODE_GUARD_115ad9fa08f9b8368045cfda498ff99b

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


#include "PositionPdx.hpp"
#define GFIGNORE(X) X
#define GFEXCLUDE
#define GFID
#define GFARRAYSIZE(X)
#define GFARRAYELEMSIZE(X)
using namespace apache::geode::client;

namespace AutoPdxTests {

class GFIGNORE(TESTOBJECT_EXPORT) PortfolioPdx : public PdxSerializable {
 private:
  GFID int32_t ID;

  GFID char* pkid;

  GFID PositionPdxPtr position1;
  GFID PositionPdxPtr position2;
  GFID CacheableHashMapPtr positions;
  GFID char *type, *status;
  GFID char** names;
  GFARRAYSIZE(names) int32_t nameStrArrayLen;
  // GFEXCLUDE static  const char* secIds[]; // bug #914
  GFEXCLUDE static char* secIds[];
  GFID int8_t* newVal;
  GFARRAYSIZE(newVal) int32_t newValSize;
  GFID CacheableDatePtr creationDate;
  GFID int8_t *arrayNull, *arrayZeroSize;
  GFARRAYSIZE(arrayNull) int32_t arrayNullLen;
  GFARRAYSIZE(arrayZeroSize) int32_t arrayZeroSizeLen;

 public:
  PortfolioPdx()
      : ID(0),
        pkid(NULL),
        type(NULL),
        status(NULL),
        newVal(NULL),
        creationDate(NULLPTR),
        arrayNull(NULL),
        arrayZeroSize(NULL) {}

  PortfolioPdx(int32_t id, int32_t size = 0, char** nm = NULL);

  virtual ~PortfolioPdx();

  int32_t getID() { return ID; }

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

  static PdxSerializable* createDeserializable();

  const char* getClassName() const;

  using PdxSerializable::toData;
  using PdxSerializable::fromData;
  virtual void toData(PdxWriterPtr pw);
  virtual void fromData(PdxReaderPtr pr);

  CacheableStringPtr toString() const;
};

typedef SharedPtr<PortfolioPdx> PortfolioPdxPtr;
}  // namespace AutoPdxTests

#endif // APACHE_GEODE_GUARD_115ad9fa08f9b8368045cfda498ff99b
