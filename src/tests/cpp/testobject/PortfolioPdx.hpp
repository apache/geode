#pragma once

#ifndef APACHE_GEODE_GUARD_c671317d09616c448db78f20e3208e41
#define APACHE_GEODE_GUARD_c671317d09616c448db78f20e3208e41

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

using namespace apache::geode::client;

namespace testobject {

class TESTOBJECT_EXPORT PortfolioPdx : public PdxSerializable {
 private:
  int32_t id;

  char* pkid;

  PositionPdxPtr position1;
  PositionPdxPtr position2;
  CacheableHashMapPtr positions;
  char* type;
  char* status;
  char** names;
  static const char* secIds[];
  int8_t* newVal;
  int32_t newValSize;
  CacheableDatePtr creationDate;
  int8_t* arrayNull;
  int8_t* arrayZeroSize;

 public:
  PortfolioPdx()
      : id(0),
        pkid(NULL),
        type(NULL),
        status(NULL),
        newVal(NULL),
        creationDate(NULLPTR),
        arrayNull(NULL),
        arrayZeroSize(NULL) {}

  PortfolioPdx(int32_t id, int32_t size = 0, char** nm = NULL);

  virtual ~PortfolioPdx();

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

  static PdxSerializable* createDeserializable() { return new PortfolioPdx(); }

  const char* getClassName() const { return "testobject.PortfolioPdx"; }

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void toData(PdxWriterPtr pw);
  virtual void fromData(PdxReaderPtr pr);

  CacheableStringPtr toString() const;
};

typedef SharedPtr<PortfolioPdx> PortfolioPdxPtr;
}  // namespace testobject

#endif // APACHE_GEODE_GUARD_c671317d09616c448db78f20e3208e41
