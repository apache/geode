/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "CacheHelper.hpp"
#include "Ch_10_ContinuousQuerying.hpp"
#include <gfcpp/SelectResultsIterator.hpp>
#include <gfcpp/CqQuery.hpp>
#include <gfcpp/CqAttributesFactory.hpp>

using namespace gemfire;
using namespace docExample;

TradeOrder::~TradeOrder()
{
}

TradeOrder::TradeOrder(double price, uint32_t size, CacheableStringArrayPtr nm)
{
  price = price;
  newValSize = size;
  names = nm;
  pkid = NULLPTR;
  status = (char*)"";
  type = NULLPTR;
  newVal = 0;
  creationDate = NULLPTR;
  arrayZeroSize = NULL;
  arrayNull = NULL;
}

void TradeOrder::toData( DataOutput& output ) const
{
  output.writeDouble(price);
  output.writeObject(pkid);
  output.writeObject(type);
  output.writeUTF(status);
  output.writeObject(names);
  output.writeBytes(newVal,newValSize+1);
  output.writeObject(creationDate);
  output.writeBytes(arrayNull,0);
  output.writeBytes(arrayZeroSize,0);
}

Serializable* TradeOrder::fromData( DataInput& input )
{
  input.readDouble(&price);
  input.readObject(pkid);
  input.readObject(type);
  input.readUTF(&status);
  input.readObject(names);
  input.readBytes(&newVal,&newValSize);
  input.readObject(creationDate);
  int tmp = 0;
  input.readBytes(&arrayNull,&tmp);
  input.readBytes(&arrayZeroSize,&tmp);
  return this;
}

CacheableStringPtr TradeOrder:: toString() const
{
  char idbuf[1024];
  sprintf(idbuf,"TradeOrderObject: [ price=%g ]",price);
  char pkidbuf[1024];
  if (pkid != NULLPTR) {
    sprintf(pkidbuf, " status=%s type=%s pkid=%s\n", this->status,
      this->type->toString(), this->pkid->asChar());
  }
  else {
    sprintf(pkidbuf, " status=%s type=%s pkid=%s\n", this->status,
      this->type->toString(), this->pkid->asChar());
  }
  char creationdatebuf[2048];
  if (creationDate != NULLPTR) {
    sprintf(creationdatebuf, "creation Date %s",
      creationDate->toString()->asChar());
  }
  else {
    sprintf(creationdatebuf, "creation Date %s", "NULL");
  }

  char stringBuf[7000];
  sprintf(stringBuf, "%s%s%s",idbuf,pkidbuf,creationdatebuf);
  return CacheableString::create( stringBuf );
}

//constructor
ContinuousQuerying::ContinuousQuerying()
{
}

//destructor
ContinuousQuerying::~ContinuousQuerying()
{
}

//start cacheserver
void ContinuousQuerying::startServer()
{
  CacheHelper::initServer( 1, "tradeOrder.xml" );
}

//stop cacheserver
void ContinuousQuerying::stopServer()
{
  CacheHelper::closeServer( 1 );
}

//initialize the region.
void ContinuousQuerying::initRegion()
{
  // Create a region.
  RegionFactoryPtr regionFactory = cachePtr->createRegionFactory(CACHING_PROXY);
  regionPtr = regionFactory->setLruEntriesLimit( 20000 )
      ->setInitialCapacity( 20000 )
      ->create("tradeOrder");
}

/**
* @brief Example 10.3 CQ Creation, Execution, and Close (C++).
* Get cache and queryServicePtr - refs to local cache and QueryService
* Create client /tradeOrder region configured to talk to the server
* Create CqAttribute using CqAttributeFactory
*/
void ContinuousQuerying::example_10_3()
{
  // Get cache and queryServicePtr - refs to local cache and QueryService
  // Create client /tradeOrder region configured to talk to the server
  // Create CqAttribute using CqAttributeFactory
  CqAttributesFactory cqf;
  // Create a listener and add it to the CQ attributes
  //callback defined below
  CqListenerPtr tradeEventListener (new TradeEventListener());
  // Get the QueryService from the Cache.
  QueryServicePtr qrySvcPtr = cachePtr->getQueryService();
  cqf.addCqListener(tradeEventListener);
  CqAttributesPtr cqa = cqf.create();
  // Name of the CQ and its query
  const char* cqName = "priceTracker";
  const char* queryStr = "SELECT * FROM /tradeOrder t where t.price > 100.00";
  // Create the CqQuery
  CqQueryPtr priceTracker = qrySvcPtr->newCq(cqName, queryStr, cqa);
  try {
    // Execute CQ
    priceTracker->execute();
  } catch (Exception& ex){
    ex.printStackTrace();
  }
  // Now the CQ is running on the server, sending CqEvents to the listener
  //. . .
  //}
  // End of life for the CQ - clear up resources by closing
  priceTracker->close();
}

int main(int argc, char* argv[])
{
  try {
    printf("\nContinuousQuerying EXAMPLES: Starting...");
    ContinuousQuerying cp10;
    printf("\nContinuousQuerying EXAMPLES: Starting server...");
    cp10.startServer();
    CacheHelper::connectToDs(cp10.cacheFactoryPtr);
    printf("\nContinuousQuerying EXAMPLES: Init Cache...");
    CacheHelper::initCache(cp10.cacheFactoryPtr, cp10.cachePtr);
    printf("\nContinuousQuerying EXAMPLES: Init Region...");
    cp10.initRegion();

    Serializable::registerType( TradeOrder::createDeserializable);
    printf("Registered Serializable Query Objects");

    // populated region with TradeOrder objects.
    TradeOrderPtr trade1Ptr(new TradeOrder(100.00 /*price*/, 10 /*size*/));
    TradeOrderPtr trade2Ptr(new TradeOrder(200.00 /*price*/, 20 /*size*/));
    TradeOrderPtr trade3Ptr(new TradeOrder(300.00 /*price*/, 30 /*size*/));
    cp10.regionPtr->put("Key1", trade1Ptr);
    cp10.regionPtr->put("Key2", trade2Ptr);
    cp10.regionPtr->put("Key3", trade3Ptr);
    printf("\nContinuousQuerying EXAMPLES: Running example 10.3...");
    cp10.example_10_3();
    CacheHelper::cleanUp(cp10.cachePtr);
    printf("\nContinuousQuerying EXAMPLES: stopping server...");
    cp10.stopServer();
    printf("\nContinuousQuerying EXAMPLES: All Done.");
  }catch (const Exception & excp)
  {
    printf("\nEXAMPLES: %s: %s", excp.getName(), excp.getMessage());
    exit(1);
  }
  catch(...)
  {
    printf("\nEXAMPLES: Unknown exception");
    exit(1);
  }
  return 0;
}
