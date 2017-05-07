/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "CacheHelper.hpp"
#include "Ch_09_RemoteQuerying.hpp"
#include <testobject/Portfolio.hpp>
#include <testobject/Position.hpp>
#include <SelectResults.hpp>
#include <ResultSet.hpp>
#include <StructSet.hpp>

using namespace gemfire;
using namespace docExample;
using namespace testobject;

//constructor
RemoteQuerying::RemoteQuerying()
{
}

//destructor
RemoteQuerying::~RemoteQuerying()
{
}

//start cacheserver
void RemoteQuerying::startServer()
{
  CacheHelper::initServer( 1, "serverRemoteQuery.xml" );
}

//stop cacheserver
void RemoteQuerying::stopServer()
{
  CacheHelper::closeServer( 1 );
}

//Initalize Region
void RemoteQuerying::initRegion()
{
  /** Get the region */
  Serializable::registerType(Portfolio::createDeserializable);
  Serializable::registerType(Position::createDeserializable);
  regionPtr = cachePtr->getRegion("Portfolios");
  for (int item = 0; item < 10; item++)
  {
    CacheableStringPtr values[2];
    values[0] = CacheableString::create("name1");
    values[1] = CacheableString::create("name2");
    CacheableStringArrayPtr names = CacheableStringArray::create(values, 2);
    PortfolioPtr port(new Portfolio(item, 10, names));

    regionPtr->put(item, port);
  }

  posRegion = cachePtr->getRegion("Positions");

  for (int item=0; item < 10; item++)
  {
    PositionPtr pos(new Position(item));
    posRegion->put(item, pos);
  }
}

/**
* @brief Example 9.2 Retrieve all active portfolios.
* The following examples provide a sampling of the queries that you could run against
* /portfolios on the server.
* A query response timeout parameter of 10 seconds is specified for the execute method
* to allow sufficient time for the operation to succeed.
*/
void RemoteQuerying::example_9_2()
{
  QueryServicePtr qrySvcPtr = cachePtr->getQueryService("examplePool");
  QueryPtr qry = qrySvcPtr->newQuery("select distinct * from /Portfolios where status = 'active'");
  SelectResultsPtr resultsPtr = qry->execute(10);
  SelectResultsIterator iter = resultsPtr->getIterator();
  while(iter.hasNext())
  {
    PortfolioPtr portfolio = dynCast<PortfolioPtr >(iter.next());
  }
}

/**
* @brief Example 9.3 Retrieve all portfolios that are active and have type xyz.
* The type attribute is passed to the query engine in double quotes to distinguish it from the
* query keyword of the same name.
* A query response timeout parameter of 10 seconds is specified for the execute method
* to allow sufficient time for the operation to succeed.
*/
void RemoteQuerying::example_9_3()
{
  QueryServicePtr qrySvcPtr = cachePtr->getQueryService("examplePool");
  QueryPtr qry = qrySvcPtr->newQuery("select distinct * from	/Portfolios where status = 'active' and \"type\"='xyz'");
  SelectResultsPtr results = qry->execute(10);
  SelectResultsIterator iter = results->getIterator();
  while(iter.hasNext())
  {
    PortfolioPtr portfolio = dynCast<PortfolioPtr >(iter.next());
  }
}

/**
* @brief Example 9.4 Get the ID and status of all portfolios with positions in secId �yyy�.
*/
void RemoteQuerying::example_9_4()
{
  QueryServicePtr qrySvcPtr = cachePtr->getQueryService("examplePool");
  QueryPtr qry = qrySvcPtr->newQuery("import javaobject.Position; SELECT DISTINCT ID, status FROM "
    "/Portfolios WHERE NOT (SELECT DISTINCT * FROM positions.values "
    "posnVal TYPE Position WHERE posnVal.secId='DELL').isEmpty");
  SelectResultsPtr results = qry->execute(10);
  SelectResultsIterator iter = results->getIterator();
  while(iter.hasNext())
  {
    Struct * si = (Struct*) iter.next().ptr();
    SerializablePtr id = si->operator[]("ID");
    SerializablePtr status = si->operator[]("status");
    printf("\nID=%s, status=%s", id->toString()->asChar(), status->toString()->asChar());
  }
}

/**
* @brief Example 9.14 Query Returning a ResultSet for a Built-In Data Type.
* The following API examples demonstrate methods for returning ResultSet for both built-in and
* user defined data types.
*/
void RemoteQuerying::example_9_14()
{
  //#include "selectresults.hpp"
  //#include "resultset.hpp"

  QueryServicePtr qrySvcPtr = cachePtr->getQueryService("examplePool");
  QueryPtr query = qrySvcPtr->newQuery("select distinct pkid from /Portfolios");
  //specify 10 seconds for the query timeout period
  SelectResultsPtr results = query->execute(10);
  if (results == NULLPTR) { printf( "\nNo results returned from the server"); }
  //obtaining a handle to resultset
  ResultSetPtr rs(dynamic_cast<ResultSet*> (results.ptr()));
  if (rs == NULLPTR) { printf ("\nResultSet is not obtained \n"); return; }
  //iterating through the resultset using row index.
  for (int32_t row=0; row < rs->size(); row++) {
    SerializablePtr ser((*rs)[row]);
    CacheableStringPtr str(dynamic_cast<CacheableString*> (ser.ptr()));
    if (str != NULLPTR) {
      printf("\n string column contains - %s \n", str->asChar() );
    }
  }//end of rows
}

/**
* @brief Example 9.15 Query Returning a ResultSet for a User-Defined Data Type.
*/
void RemoteQuerying::example_9_15()
{
  //#include "selectresults.hpp"
  //#include "resultset.hpp"
  //#include "selectresultsIterator.hpp"
  //#include "Portfolio.hpp"

  QueryServicePtr qrySvcPtr = cachePtr->getQueryService("examplePool");
  const char * querystring = "select distinct * from /Portfolios";
  QueryPtr query = qrySvcPtr->newQuery(querystring);
  //specify 10 seconds for the query timeout period
  SelectResultsPtr results = query->execute(10);
  if (results == NULLPTR) { printf( "\nNo results returned from the server"); }
  //obtaining a handle to resultset
  ResultSetPtr rs(dynamic_cast<ResultSet*> (results.ptr()));
  if (rs == NULLPTR) { printf ("\nResultSet is not obtained \n"); return; }
  //iterating through the resultset using iterators.
  SelectResultsIterator iter = rs->getIterator();
  while (iter.hasNext()) {
    SerializablePtr ser = iter.next();
    PortfolioPtr port(dynamic_cast<Portfolio*> (ser.ptr()));
    if (port != NULLPTR) {
      printf("\nPortfolio object is - %s \n", port->toString()->asChar() );
    }
  }//end of rows
}

/**
* @brief Example 9.16 Query Returning a StructSet for a Built-In Data Type.
* The following examples return a StructSet for built-in and user-defined data types, Struct
* objects,and collections.
*/
void RemoteQuerying::example_9_16()
{
  //#include "selectResults.hpp"
  //#include "resultSet.hpp"

  QueryServicePtr qrySvcPtr = cachePtr->getQueryService("examplePool");
  const char * querystring = "select distinct ID, pkid, status, getType from /Portfolios";
  QueryPtr query = qrySvcPtr->newQuery(querystring);
  //specify 10 seconds for the query timeout period
  SelectResultsPtr results = query->execute(10);
  if (results == NULLPTR) { printf( "\nNo results returned from the server"); }
  //obtaining a handle to resultset
  StructSetPtr ss(dynamic_cast<StructSet*> (results.ptr()));
  if (ss == NULLPTR) { printf ("\nStructSet is not obtained \n"); return; }
  //iterating through the resultset using indexes.
  for ( int32_t row=0; row < ss->size();  row++) {
    Struct * siptr = (Struct*) dynamic_cast<Struct*> ( ((*ss)[row]).ptr() );
    if (siptr == NULL) { printf("\nstruct is empty \n"); continue;}
    //iterate through fields now
    for( int32_t field=0; field < siptr->length(); field++) {
      SerializablePtr fieldptr((*siptr)[field]);
      if(fieldptr == NULLPTR ) { printf("\nnull data received\n"); }
      CacheableStringPtr str(dynamic_cast<CacheableString*>(fieldptr.ptr()));
      if (str == NULLPTR) { printf("\n field is of some other type \n"); }
      else printf("\n Data for %s is %s ", siptr->getFieldName(field), str->asChar() );
    } //end of columns
  }//end of rows
}

/**
* @brief Example 9.17 Query Returning a StructSet for a User-Defined Data Type.
*/
void RemoteQuerying::example_9_17()
{
  //#include "selectResults.hpp"
  //#include "structSet.hpp"
  //#include "Portfolio.hpp"
  QueryServicePtr qrySvcPtr = cachePtr->getQueryService("examplePool");
  const char * querystring =
      "select distinct port.ID, port from /Portfolios port";
  QueryPtr query = qrySvcPtr->newQuery(querystring);
  //specify 10 seconds for the query timeout period
  SelectResultsPtr results = query->execute(10);
  if (results == NULLPTR) { printf( "\nNo results returned from the server"); }
  //obtaining a handle to resultset
  StructSetPtr ss(dynamic_cast<StructSet*> (results.ptr()));
  if (ss == NULLPTR) { printf ("\nStructSet is not obtained \n"); return; }
  //iterating through the resultset using indexes.
  for ( int32_t row=0; row < ss->size();  row++) {
    Struct * siptr = (Struct*) dynamic_cast<Struct*> ( ((*ss)[row]).ptr() );
    if (siptr == NULL) { printf("\nstruct is empty \n"); continue; }
    //iterate through fields now
    for( int32_t field=0; field < siptr->length(); field++) {
      SerializablePtr fieldptr((*siptr)[field]);
      if(fieldptr == NULLPTR ) { printf("\nnull data received\n"); }
      CacheableStringPtr str(dynamic_cast<CacheableString*>(fieldptr.ptr()));
      if (str != NULLPTR) {
        printf("\n Data for %s is %s ", siptr->getFieldName(field), str->asChar() );
      }
      else {
        PortfolioPtr port(dynamic_cast<Portfolio*> (fieldptr.ptr()));
        if (port == NULLPTR) { printf("\n field is of some other type \n"); }
        else printf("\nPortfolio data for %s is %s \n", siptr->getFieldName(field),port->toString()->asChar());
      }
    } //end of columns
  }//end of rows
}

/**
* @brief Example 9.18 Returning Struct Objects.
*/
void RemoteQuerying::example_9_18()
{
  //#include "selectResults.hpp"
  //#include "structSet.hpp"
  QueryServicePtr qrySvcPtr = cachePtr->getQueryService("examplePool");
  const char * querystring =
      "select distinct derivedProjAttrbts, key: p.key from "
        "/Portfolios.entries p, (select distinct x.ID, myPos.secId from "
    "/Portfolios x, x.positions.values as myPos) derivedProjAttrbts where "
    "p.value.ID = derivedProjAttrbts.ID and derivedProjAttrbts.secId = 'IBM'";
  QueryPtr query = qrySvcPtr->newQuery(querystring);
  //specify 10 seconds for the query timeout period
  SelectResultsPtr results = query->execute(10);
  if (results == NULLPTR) { printf( "\nNo results returned from the server"); }
  //obtaining a handle to resultset
  StructSetPtr ss(dynamic_cast<StructSet*> (results.ptr()));
  if (ss == NULLPTR) { printf ("\nStructSet is not obtained \n"); return; }
  //iterating through the resultset using indexes.
  for ( int32_t row=0; row < ss->size();  row++) {
    Struct * siptr = (Struct*) dynamic_cast<Struct*> ( ((*ss)[row]).ptr() );
    if (siptr == NULL) { printf("\nstruct is empty \n"); }
    //iterate through fields now
    for( int32_t field=0; field < siptr->length(); field++) {
      SerializablePtr fieldptr((*siptr)[field]);
      if(fieldptr == NULLPTR ) { printf("\nnull data received\n"); }
      CacheableStringPtr str(dynamic_cast<CacheableString*>(fieldptr.ptr()));
      if (str != NULLPTR) {
        printf("\n Data for %s is %s ", siptr->getFieldName(field),
          str->asChar() );
      }
      else {
        StructPtr simpl(dynamic_cast<Struct*> (fieldptr.ptr()));
        if (simpl == NULLPTR) { printf("\n field is of some other type \n"); continue; }
        printf( "\n struct received %s \n", siptr->getFieldName(field) );
        for( int32_t inner_field=0; inner_field < simpl->length(); inner_field++ ) {
          SerializablePtr innerfieldptr((*simpl)[inner_field]);
          if (innerfieldptr == NULLPTR) { printf("\nfield of struct is NULL\n"); }
          CacheableStringPtr str(dynamic_cast<CacheableString*>(innerfieldptr.ptr()));
          if (str != NULLPTR) {
            printf("\n Data for %s is %s ",
              simpl->getFieldName(inner_field),str->asChar() );
          }
          else { printf("\n some other object type inside struct\n"); }
        }
      }
    } //end of columns
  }//end of rows
}

/**
* @brief Example 9.19 Returning Collections.
*/
void RemoteQuerying::example_9_19()
{
  //#include "selectResults.hpp"
  //#include "structSet.hpp"
  //#include "Portfolio.hpp"
  QueryServicePtr qrySvcPtr = cachePtr->getQueryService("examplePool");
  const char * querystring = "select distinct ID, names from /Portfolios";
  QueryPtr query = qrySvcPtr->newQuery(querystring);
  //specify 10 seconds for the query timeout period
  SelectResultsPtr results = query->execute(10);
  if (results == NULLPTR) { printf( "\nNo results returned from the server"); }
  //obtaining a handle to resultset
  StructSetPtr ss(dynamic_cast<StructSet*> (results.ptr()));
  if (ss == NULLPTR) { printf ("\nStructSet is not obtained \n"); return; }
  //iterating through the resultset using indexes.
  for ( int32_t row=0; row < ss->size();  row++) {
    Struct * siptr = dynamic_cast<Struct*> ( ((*ss)[row]).ptr() );
    if (siptr == NULL) { printf("\nstruct is empty \n"); continue; }
    //iterate through fields now
    for( int32_t field=0; field < siptr->length(); field++) {
      SerializablePtr fieldptr((*siptr)[field]);
      if(fieldptr == NULLPTR ) { printf("\nnull data received\n"); }
      CacheableStringPtr str(dynamic_cast<CacheableString*>(fieldptr.ptr()));
      if (str != NULLPTR) {
        printf("\n Data for %s is %s ", siptr->getFieldName(field),
          str->asChar() );
      }
      else {
        CacheableObjectArrayPtr coa(dynamic_cast<CacheableObjectArray*>(fieldptr.ptr()));
        if (coa == NULLPTR) { printf("\n field is of some other type\n"); continue; }
        printf( "\n objectArray received %s \n", siptr->getFieldName(field) );
        for(unsigned arrlen=0; arrlen < (uint32_t)coa->length(); arrlen++)
        {
          printf("\n Data for %s is %s ",siptr->getFieldName(field),
            coa->operator[](arrlen)->toString()->asChar());
        }
      }
    } //end of columns
  }//end of rows
}

int main(int argc, char* argv[])
{
  try {
    printf("\nRemoteQuerying EXAMPLES: Starting...");
    RemoteQuerying cp9;
    printf("\nRemoteQuerying EXAMPLES: Starting server...");
    cp9.startServer();
    printf("\nRemoteQuerying EXAMPLES: Connect to DS...");
    CacheHelper::connectToDs(cp9.cacheFactoryPtr, "/clientRemoteQuery.xml");
    printf("\nRemoteQuerying EXAMPLES: Init Cache...");
    CacheHelper::initCache(cp9.cacheFactoryPtr, cp9.cachePtr, "/clientRemoteQuery.xml");
    printf("\nRemoteQuerying EXAMPLES: Init Region...");
    cp9.initRegion();

    printf("\nRemoteQuerying EXAMPLES: Running example 9.2...");
    cp9.example_9_2();
    printf("\nRemoteQuerying EXAMPLES: Running example 9.3...");
    cp9.example_9_3();
    printf("\nRemoteQuerying EXAMPLES: Running example 9.4...");
    cp9.example_9_4();
    printf("\nRemoteQuerying EXAMPLES: Running example 9.14...");
    cp9.example_9_14();
    printf("\nRemoteQuerying EXAMPLES: Running example 9.15...");
    cp9.example_9_15();
    printf("\nRemoteQuerying EXAMPLES: Running example 9.16...");
    cp9.example_9_16();
    printf("\nRemoteQuerying EXAMPLES: Running example 9.17...");
    cp9.example_9_17();
    printf("\nRemoteQuerying EXAMPLES: Running example 9.18...");
    cp9.example_9_18();
    printf("\nRemoteQuerying EXAMPLES: Running example 9.19...");
    cp9.example_9_19();
    CacheHelper::cleanUp(cp9.cachePtr);
    printf("\nRemoteQuerying EXAMPLES: stopping server...");
    cp9.stopServer();
    printf("\nRemoteQuerying EXAMPLES: All Done.");
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
