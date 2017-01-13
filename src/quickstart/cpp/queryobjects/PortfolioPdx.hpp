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

#ifndef __PORTFOLIOPDX_HPP__
#define __PORTFOLIOPDX_HPP__

#include "PositionPdx.hpp"

using namespace gemfire;

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
}
#endif
