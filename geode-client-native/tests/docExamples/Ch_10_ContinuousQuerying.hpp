/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef CONTINUOUSQUERYING_HPP_
#define CONTINUOUSQUERYING_HPP_
/*
* This is the example to verify the code snippets given in the native client guide chapter 10.
* This is the example with Continuous Querying.
*/

#include <gfcpp/gf_types.hpp>
#include "gfcpp/GemfireCppCache.hpp"

using namespace gemfire;

class TradeOrder;
typedef SharedPtr< TradeOrder > TradeOrderPtr;

/**
* @brief Example 10.1 CqListener Implementation (C++).
* The following C++ example shows how you might program a simple CqListener to
* update a display screen based on the CQ events it receives. The listener retrieves the
* queryOperation and entry key and value from the CqEvent, then updates the screen according to the
* operation type provided in queryOperation.
*/
// CqListener class
class TradeEventListener : public CqListener {
public:
  void onEvent(const CqEvent& cqEvent) {
    // Operation associated with the query op
    CqOperation::CqOperationType queryOperation =
      cqEvent.getQueryOperation();
    // key and new value from the event
    CacheableKeyPtr key = cqEvent.getKey();
    TradeOrderPtr tradeOrder =
      dynCast<TradeOrderPtr>(cqEvent.getNewValue());
    if (queryOperation==CqOperation::OP_TYPE_UPDATE) {
      // update data on the screen for the trade order
      //. . .
    }
    else if (queryOperation==CqOperation::OP_TYPE_CREATE) {
      // add the trade order to the screen
      //. . .
    }
    else if (queryOperation==CqOperation::OP_TYPE_DESTROY) {
      // remove the trade order from the screen
      //. . .
    }
  }

  void onError(const CqEvent& cqEvent) {
    // handle the error
  }

  void close() {
    // close the output screen for the trades
  }
};

class TradeOrder : public Serializable
{
private:
  double price;
  CacheableStringPtr pkid;
  CacheableStringPtr type;
  char* status;
  CacheableStringArrayPtr names;
  static const char* secIds[];
  uint8_t* newVal;
  int32_t newValSize;
  CacheableDatePtr creationDate;
  uint8_t* arrayNull;
  uint8_t* arrayZeroSize;

  inline uint32_t getObjectSize( const SerializablePtr& obj ) const
  {
    return (obj == NULLPTR ? 0 : obj->objectSize());
  }

public:
  TradeOrder(): price( 0 ), pkid(NULLPTR), type(NULLPTR), status(NULL),
    newVal(NULL), creationDate(NULLPTR), arrayNull(NULL), arrayZeroSize(NULL) { }
  TradeOrder(double price, uint32_t size=0, CacheableStringArrayPtr nm=NULLPTR);
  virtual ~TradeOrder();

  virtual uint32_t objectSize() const
  {
    uint32_t objectSize = sizeof(TradeOrder);
    objectSize += getObjectSize( pkid );
    objectSize += getObjectSize( type );
    objectSize += (uint32_t)(status==NULL ? 0 : sizeof(char)*strlen(status));
    objectSize += getObjectSize( names );
    objectSize += sizeof(uint8_t) * newValSize;
    objectSize += getObjectSize(creationDate);
    return objectSize;
  }

  double getprice() {
    return price;
  }
  void showNames(const char* label)
  {
    printf("label is %s:",label);
    if(names==NULLPTR)
    {
      printf("names is NULL");
      return;
    }
    for(int i = 0; i < names->length(); i++)
    {
      printf("names[%d]=%s", i, names->operator[](i)->asChar());
    }
  }

  CacheableStringPtr getPkid() {
    return pkid;
  }

  bool testMethod(bool booleanArg) {
    return true;
  }

  char* getStatus() {
    return status;
  }

  bool isActive() {
    return ( strcmp(status, "active") == 0 ) ? true : false;
  }

  uint8_t* getNewVal() {
    return newVal;
  }

  int32_t getNewValSize()
  {
    return newValSize;
  }

  CacheableStringPtr getType() {
    return this->type;
  }

  CacheableDatePtr getCreationDate() {
    return creationDate;
  }

  uint8_t* getArrayNull() {
    return arrayNull;
  }

  uint8_t* getArrayZeroSize() {
    return arrayZeroSize;
  }

  static Serializable* createDeserializable( ){
    return new TradeOrder();
  }

  virtual void toData( DataOutput& output ) const;
  virtual Serializable* fromData( DataInput& input );
  virtual int32_t classId( ) const { return 0x04; }
  CacheableStringPtr toString() const;
};

class ContinuousQuerying
{
public:
  ContinuousQuerying();
  ~ContinuousQuerying();
  void connectToDs();
  void initCache();
  void initRegion();
  void cleanUp();
  void startServer();
  void stopServer();
  void example_10_3();
public:
  CacheFactoryPtr cacheFactoryPtr; //for examples.
  CachePtr cachePtr; //for examples.
  RegionPtr regionPtr; //for examples.
};

#endif /* CONTINUOUSQUERYING_HPP_ */
