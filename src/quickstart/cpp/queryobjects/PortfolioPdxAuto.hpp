/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
