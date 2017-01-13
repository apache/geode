/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef TEST_QUERYHELPER_HPP
#define TEST_QUERYHELPER_HPP

#include <gfcpp/GemfireCppCache.hpp>
#include <stdlib.h>
#include <gfcpp/SystemProperties.hpp>
#include <ace/OS.h>

#include "DistributedSystemImpl.hpp"

#include "testobject/Portfolio.hpp"
#include "testobject/Position.hpp"
#include "testobject/PdxType.hpp"
#include "testobject/PortfolioPdx.hpp"
#include "testobject/PositionPdx.hpp"
#include <gfcpp/ResultSet.hpp>
#include <gfcpp/StructSet.hpp>
//#include <gfcpp/Struct.hpp>

//#ifndef ROOT_NAME
// ROOT_NAME+++ DEFINE ROOT_NAME before including QueryHelper.hpp
//#endif

#ifndef ROOT_SCOPE
#define ROOT_SCOPE LOCAL
#endif

using namespace gemfire;
using namespace testData;
using namespace PdxTests;
using namespace testobject;
class QueryHelper {
 public:
  static QueryHelper* singleton;

  static QueryHelper& getHelper() {
    if (singleton == NULL) {
      singleton = new QueryHelper();
    }
    return *singleton;
  }

  QueryHelper() {
    portfolioSetSize = 20;
    portfolioNumSets = 1;
    positionSetSize = 20;
    positionNumSets = 1;
  }

  virtual ~QueryHelper() { ; }

  virtual void populatePortfolioData(RegionPtr& pregion, int setSize,
                                     int numSets, int32_t objSize = 1,
                                     CacheableStringArrayPtr nm = NULLPTR);
  virtual void populatePositionData(RegionPtr& pregion, int setSize,
                                    int numSets);
  virtual void populatePortfolioPdxData(RegionPtr& pregion, int setSize,
                                        int numSets, int32_t objSize = 1,
                                        char** nm = NULL);
  virtual void populatePositionPdxData(RegionPtr& pregion, int setSize,
                                       int numSets);
  virtual void populatePDXObject(RegionPtr& pregion);
  virtual void getPDXObject(RegionPtr& pregion);

  virtual bool verifyRS(SelectResultsPtr& resultset, int rowCount);
  virtual bool verifySS(SelectResultsPtr& structset, int rowCount,
                        int fieldCount);

  // utility methods
  virtual int getPortfolioSetSize() { return portfolioSetSize; };
  virtual int getPortfolioNumSets() { return portfolioNumSets; };
  virtual int getPositionSetSize() { return positionSetSize; };
  virtual int getPositionNumSets() { return positionNumSets; };

  bool isExpectedRowsConstantRS(int queryindex) {
    for (int i = (sizeof(constantExpectedRowsRS) / sizeof(int)) - 1; i > -1;
         i--)
      if (constantExpectedRowsRS[i] == queryindex) {
        printf("index %d is having constant rows \n",
               constantExpectedRowsRS[i]);
        return true;
      }

    return false;
  }

  bool isExpectedRowsConstantPQRS(int queryindex) {
    for (int i = (sizeof(constantExpectedRowsPQRS) / sizeof(int)) - 1; i > -1;
         i--)
      if (constantExpectedRowsPQRS[i] == queryindex) {
        printf("index %d is having constant rows \n",
               constantExpectedRowsPQRS[i]);
        return true;
      }

    return false;
  }

  bool isExpectedRowsConstantSS(int queryindex) {
    for (int i = (sizeof(constantExpectedRowsSS) / sizeof(int)) - 1; i > -1;
         i--) {
      if (constantExpectedRowsSS[i] == queryindex) {
        printf("index %d is having constant rows \n",
               constantExpectedRowsSS[i]);
        return true;
      }
    }

    return false;
  }

  bool isExpectedRowsConstantSSPQ(int queryindex) {
    for (int i = (sizeof(constantExpectedRowsSSPQ) / sizeof(int)) - 1; i > -1;
         i--) {
      if (constantExpectedRowsSSPQ[i] == queryindex) {
        printf("index %d is having constant rows \n",
               constantExpectedRowsSSPQ[i]);
        return true;
      }
    }
    return false;
  }

 private:
  int portfolioSetSize;
  int portfolioNumSets;
  int positionSetSize;
  int positionNumSets;
};

QueryHelper* QueryHelper::singleton = NULL;

//===========================================================================================

void QueryHelper::populatePortfolioData(RegionPtr& rptr, int setSize,
                                        int numSets, int32_t objSize,
                                        CacheableStringArrayPtr nm) {
  // lets reset the counter for uniform population of position objects
  Position::resetCounter();

  for (int set = 1; set <= numSets; set++) {
    for (int current = 1; current <= setSize; current++) {
      CacheablePtr port(new Portfolio(current, objSize, nm));

      char portname[100] = {0};
      ACE_OS::sprintf(portname, "port%d-%d", set, current);

      CacheableKeyPtr keyport = CacheableKey::create(portname);
      // printf(" QueryHelper::populatePortfolioData creating key = %s and
      // puting data \n",portname);
      rptr->put(keyport, port);
    }
  }
  // portfolioSetSize = setSize; portfolioNumSets = numSets; objectSize =
  // objSize;

  printf("all puts done \n");
}

const char* secIds[] = {"SUN", "IBM",  "YHOO", "GOOG", "MSFT",
                        "AOL", "APPL", "ORCL", "SAP",  "DELL"};

void QueryHelper::populatePositionData(RegionPtr& rptr, int setSize,
                                       int numSets) {
  int numSecIds = sizeof(secIds) / sizeof(char*);

  for (int set = 1; set <= numSets; set++) {
    for (int current = 1; current <= setSize; current++) {
      CacheablePtr pos(
          new Position(secIds[current % numSecIds], current * 100));

      char posname[100] = {0};
      ACE_OS::sprintf(posname, "pos%d-%d", set, current);

      CacheableKeyPtr keypos = CacheableKey::create(posname);
      rptr->put(keypos, pos);
    }
  }
  // positionSetSize = setSize; positionNumSets = numSets;
}

void QueryHelper::populatePortfolioPdxData(RegionPtr& rptr, int setSize,
                                           int numSets, int32_t objSize,
                                           char** nm) {
  // lets reset the counter for uniform population of position objects
  PositionPdx::resetCounter();

  for (int set = 1; set <= numSets; set++) {
    for (int current = 1; current <= setSize; current++) {
      CacheablePtr port(new PortfolioPdx(current, objSize));

      char portname[100] = {0};
      ACE_OS::sprintf(portname, "port%d-%d", set, current);

      CacheableKeyPtr keyport = CacheableKey::create(portname);

      rptr->put(keyport, port);
      LOGINFO("populatePortfolioPdxData:: Put for iteration current = %d done",
              current);
    }
  }
  // portfolioSetSize = setSize; portfolioNumSets = numSets; objectSize =
  // objSize;

  printf("all puts done \n");
}

void QueryHelper::populatePositionPdxData(RegionPtr& rptr, int setSize,
                                          int numSets) {
  int numSecIds = sizeof(secIds) / sizeof(char*);

  for (int set = 1; set <= numSets; set++) {
    for (int current = 1; current <= setSize; current++) {
      CacheablePtr pos(
          new PositionPdx(secIds[current % numSecIds], current * 100));

      char posname[100] = {0};
      ACE_OS::sprintf(posname, "pos%d-%d", set, current);

      CacheableKeyPtr keypos = CacheableKey::create(posname);
      rptr->put(keypos, pos);
    }
  }
  // positionSetSize = setSize; positionNumSets = numSets;
}

void QueryHelper::populatePDXObject(RegionPtr& rptr) {
  // Register PdxType Object
  Serializable::registerPdxType(PdxTests::PdxType::createDeserializable);
  LOG("PdxObject Registered Successfully....");

  // Creating object of type PdxObject
  CacheablePtr pdxobj(new PdxTests::PdxType());
  CacheableKeyPtr keyport = CacheableKey::create("ABC");

  // PUT Operation
  rptr->put(keyport, pdxobj);

  // locally destroy PdxObject
  rptr->localDestroy(keyport);
  LOG("localDestroy() operation....Done");

  // Remote GET for PdxObject
  // PdxObject *obj2 = dynamic_cast<PdxObject *> ((rptr->get(keyport)).ptr());
  PdxTests::PdxTypePtr obj2 = dynCast<PdxTests::PdxTypePtr>(rptr->get(keyport));

  LOGINFO("get... Result-1: Returned float=%f, String val = %s double=%lf",
          obj2->getFloat(), obj2->getString(), obj2->getDouble());
  // LOGINFO("get.. Result-2: Returned BOOL = %d and BYTE = %s SHORT=%d INT=%d",
  // obj2->getBool(), obj2->getByte(), obj2->getShort(), obj2->getInt());

  // TODO
  /*
  ASSERT(obj2->getID1() == 101, "ID1 = 101 expected");
  ASSERT(obj2->getID2() == 201, "ID2 = 201 expected");
  ASSERT(obj2->getID3() == 301, "ID3 = 301 expected");
  */

  LOG("NIL:200:PUT Operation successfully Done....End");
}

void QueryHelper::getPDXObject(RegionPtr& rptr) {
  // Remote GET for PdxObject
  // PdxObject *obj2 = dynamic_cast<PdxObject *> ((rptr->get(keyport)).ptr());

  CacheableKeyPtr keyport = CacheableKey::create("ABC");
  LOG("Client-2 PdxObject GET OP Start....");
  PdxTests::PdxTypePtr obj2 = dynCast<PdxTests::PdxTypePtr>(rptr->get(keyport));
  LOG("Client-2 PdxObject GET OP Done....");

  /*
  LOGINFO("GET OP Result: BoolVal=%d", obj2->getBool());
  LOGINFO("GET OP Result: ByteVal=%d", obj2->getByte());
  LOGINFO("GET OP Result: ShortVal=%d", obj2->getShort());*/

  // LOGINFO("GET OP Result: IntVal=%d", obj2->getInt());
  /*
  LOGINFO("GET OP Result: LongVal=%ld", obj2->getLong());
  LOGINFO("GET OP Result: FloatVal=%f", obj2->getFloat());
  LOGINFO("GET OP Result: DoubleVal=%lf", obj2->getDouble());
  LOGINFO("GET OP Result: StringVal=%s", obj2->getString());
  */
}

bool QueryHelper::verifyRS(SelectResultsPtr& resultSet, int expectedRows) {
  if (!instanceOf<ResultSetPtr>(resultSet)) {
    return false;
  }

  ResultSetPtr rsptr = staticCast<ResultSetPtr>(resultSet);

  int foundRows = 0;

  SelectResultsIterator iter = rsptr->getIterator();

  for (int32_t rows = 0; rows < rsptr->size(); rows++) {
    SerializablePtr ser = (*rsptr)[rows];
    foundRows++;
  }

  printf("found rows %d, expected %d \n", foundRows, expectedRows);
  if (foundRows == expectedRows) return true;

  return false;
}

bool QueryHelper::verifySS(SelectResultsPtr& structSet, int expectedRows,
                           int expectedFields) {
  if (!instanceOf<StructSetPtr>(structSet)) {
    if (expectedRows == 0 && expectedFields == 0)
      return true;  // quite possible we got a null set back.
    printf("we have structSet itself NULL \n");
    return false;
  }

  StructSetPtr ssptr = staticCast<StructSetPtr>(structSet);

  int foundRows = 0;

  for (SelectResults::Iterator iter = ssptr->begin(); iter != ssptr->end();
       iter++) {
    SerializablePtr ser = (*iter);
    foundRows++;

    Struct* siptr = dynamic_cast<Struct*>(ser.ptr());

    if (siptr == NULL) {
      printf("siptr is NULL \n\n");
      return false;
    }

    int foundFields = 0;

    for (int32_t cols = 0; cols < siptr->length(); cols++) {
      SerializablePtr field = (*siptr)[cols];
      foundFields++;
    }

    if (foundFields != expectedFields) {
      char buffer[1024] = {'\0'};
      sprintf(buffer, "found fields %d, expected fields %d \n", foundFields,
              expectedFields);
      LOG(buffer);
      return false;
    }
  }

  if (foundRows == expectedRows) return true;

  // lets log and return in case of error only situation
  char buffer[1024] = {'\0'};
  sprintf(buffer, "found rows %d, expected rows %d\n", foundRows, expectedRows);
  LOG(buffer);
  return false;
}

#endif  // TEST_QUERYHELPER_HPP
