/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
 * @file cacheRunner.cpp
 * @since   1.0
 * @version 1.0
 * @see
 *
 * This program demonstrates the functionality offered by the Gemfire
 * Native Client C++ API.
 */

#ifdef WIN32
#define _GS_ENABLE_WIN_MEMORY_LEAK_CHECK

#ifdef _GS_ENABLE_WIN_MEMORY_LEAK_CHECK
#include <crtdbg.h>
#include <stdio.h>
#endif

#endif

#define _GS_CACHE_RUNNER_SYSTEM     "theSystemTest"
#define _GS_CACHE_RUNNER_CACHE      "theCache"
#define _GS_CACHE_RUNNER_WITH_LLW     "listenerWriterLoader"

#include <gfcpp/GemfireCppCache.hpp>
#include <gfcpp/AttributesMutator.hpp>
#include <gfcpp/AttributesFactory.hpp>
#include <gfcpp/ScopeType.hpp>
#include <gfcpp/Query.hpp>
#include <gfcpp/QueryService.hpp>
#include <gfcpp/SelectResults.hpp>
#include <gfcpp/ResultSet.hpp>
#include <gfcpp/StructSet.hpp>
#include <gfcpp/Struct.hpp>
#include <gfcpp/SelectResultsIterator.hpp>


#include <typeinfo>

#include "CacheRunner.hpp"
#include "TestCacheListener.hpp"
#include "TestCacheLoader.hpp"
#include "TestCacheWriter.hpp"
#include "ExampleObject.hpp"
#include "Position.hpp"
#include "Portfolio.hpp"
#include "User.hpp"
#include <assert.h>

#ifdef WIN32
#include <io.h>
#define access _access
#define F_OK 0
#define R_OK 04
#define atoll _atoi64
#else
#include <unistd.h>
#endif

// ----------------------------------------------------------------------------
/**
  * @brief ExampleObject class for testing the put functionality for object
  * @brief User          class for testing the put functionality for object
  */
// ---------------------------------------------------------------------------
// ----------------------------------------------------------------------------
/**
  * Test Parses the command line and runs the <code>CacheRunner</code> example.
  */
// ----------------------------------------------------------------------------
using namespace std;
using namespace testobject;

int main (int argc, char **argv)
{
  std::string sXmlFileName;
#ifdef _GS_ENABLE_WIN_MEMORY_LEAK_CHECK
  _CrtSetDbgFlag( _CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF);
#endif

  if (argc != 2) {
    printf("Usage: CacheRunner <cache.xml>\n");
    exit(1);
  }

  sXmlFileName = argv[1];

  // Does the cache file exist?
  if (access(sXmlFileName.c_str(), F_OK) == -1){
    printf("Supplied Cache config file <cache.xml> does not exist\n");
    exit(1);
  }
  // Can we access the cache file?
  if (access(sXmlFileName.c_str(), R_OK) == -1){
    printf("Supplied Cache config file <cache.xml> can not be accessed\n");
    exit(1);
  }


try{
    CacheRunnerPtr cacheRunnerPtr = CacheRunner::create_Runner();

    cacheRunnerPtr->setXmlFile(sXmlFileName);
    cacheRunnerPtr->initialize();

    cacheRunnerPtr->go();
  } catch(OutOfMemoryException& ex)
  {
    printf("Out of Memory exception in main [%s]\n", ex.getMessage());
    exit(1);
  }

  return 0;
}

// ----------------------------------------------------------------------------
/**
  * Connect to distributed system and set properties
  * returns true if success, false if failed
  */
// ----------------------------------------------------------------------------

bool CacheRunner::connectDistributedSystem(const char* pszCacheXmlFileName)
{
  bool bSuccess = false;
  disconnectDistributedSystem( );

  try {
    Serializable::registerType( ExampleObject::createInstance);
    Serializable::registerType( User::createInstance);
    Serializable::registerType( Position::createDeserializable);
    Serializable::registerType( Portfolio::createDeserializable);
    bSuccess = true;
  } catch (IllegalArgumentException& ex)
  {
     fprintf(stderr, "Exception IllegalArgumentException in CacheRunner::initialize [%s]\n", ex.getMessage());
     exit(1);
  } catch (AlreadyConnectedException& ex)
  {
     fprintf(stderr, "Exception AlreadyConnectedException in CacheRunner::initialize [%s]\n", ex.getMessage());
     exit(1);
  }
  catch (Exception& ex) {
     fprintf(stderr, "Exception in CacheRunner::connectDistributedSystem [%s]\n", ex.getMessage());
     exit(1);
  }

  if (bSuccess){
    try{
      PropertiesPtr systemProp = Properties::create();
      systemProp->insert("cache-xml-file",pszCacheXmlFileName);
      CacheFactoryPtr cacheFactory = CacheFactory::createCacheFactory(systemProp);
      m_cachePtr =  cacheFactory->create();
      VectorOfRegion vrp;
      m_cachePtr->rootRegions(vrp);
      m_currRegionPtr = m_cachePtr->getRegion(vrp.at(vrp.size() - 1)->getName());
      m_currRegionAttributesPtr = m_currRegionPtr->getAttributes();

      RegionFactoryPtr rgnFac = m_cachePtr->createRegionFactory(CACHING_PROXY);
      if (m_currRegionAttributesPtr->getCacheListener() == NULLPTR) {
        rgnFac->setCacheListener(CacheListenerPtr(new TestCacheListener()));
      }
      if (m_currRegionAttributesPtr->getCacheLoader() == NULLPTR) {
        rgnFac->setCacheLoader(CacheLoaderPtr(new TestCacheLoader()));
      }
      if (m_currRegionAttributesPtr->getCacheWriter() == NULLPTR) {
        rgnFac->setCacheWriter(CacheWriterPtr(new TestCacheWriter()));
      }
    }
    catch (Exception& ex) {
      fprintf(stderr, "Exception in CacheRunner::connectDistributedSystem [%s]\n", ex.getMessage());
      exit(1);
    }
  }

  return bSuccess;
}

// ----------------------------------------------------------------------------
/**
  * Disconnect to distributed system and set properties
  *   return true if success
  *   return false if failed
  */
// ----------------------------------------------------------------------------

void CacheRunner::disconnectDistributedSystem( )
{
  if (m_cachePtr != NULLPTR) {
      m_cachePtr->close();
      m_cachePtr=NULLPTR;
  }
}

// ----------------------------------------------------------------------------
/**
  * Initializes the <code>Cache</code> for this example program.
  * Uses the {@link TestCacheListener}, {@link TestCacheLoader},
  * {@link TestCacheWriter}, and {@link TestCapacityController}.
  */
// ----------------------------------------------------------------------------

void CacheRunner::initialize( )
{
  if (connectDistributedSystem(m_sCacheXmlFileName.c_str())== false) {
    exit(-1);
  }
}

// ----------------------------------------------------------------------------
/**
  * Sets the <code>cache.xml</code> file used to declaratively
  * initialize the cache in this example.
  */
// ----------------------------------------------------------------------------

void CacheRunner::setXmlFile( std::string cacheXmlFileName )
{
  m_sCacheXmlFileName = cacheXmlFileName;
}

// ----------------------------------------------------------------------------
/**
  * Prompts the user for input and executes the command accordingly.
  *
  */
// ----------------------------------------------------------------------------

void CacheRunner::go( )
{
  CommandReader commandReader;

  printf("Enter 'help' or '?' for help at the Command prompt.\n");
  printf("\n");

  while (true) {
    try {

      printf("%s> ", m_currRegionPtr->getFullPath());
      fflush(stdout);
      commandReader.readCommandLineFromStdin();

        if (commandReader.isCommandStartsWith("exit") ||
          commandReader.isCommandStartsWith("quit"))
        {
          if(m_cachePtr != NULLPTR){
            m_cachePtr->close();
            m_cachePtr=NULLPTR;
          }
          exit(0);
  	    }
        else if (commandReader.isCommandStartsWith("set")) {
          setRgnAttr(commandReader);
        }
        else if (commandReader.isCommandStartsWith("putAll")) {
          putAll(commandReader);
        }
        else if (commandReader.isCommandStartsWith("put")) {
          put(commandReader);
        }
        else if (commandReader.isCommandStartsWith("create")) {
          create(commandReader);
        }
        else if (commandReader.isCommandStartsWith("get")) {
          get(commandReader);
        }
        else if (commandReader.isCommandStartsWith("run")) {
          run(commandReader);
        }
        else if (commandReader.isCommand("reg")) {
          registerKeys(commandReader);
        }
        else if (commandReader.isCommand("unreg")) {
          unregisterKeys(commandReader);
        }
        else if (commandReader.isCommand("regex")) {
          registerRegex(commandReader);
        }
        else if (commandReader.isCommand("unregex")) {
          unregisterRegex(commandReader);
        }
        else if (commandReader.isCommandStartsWith("inv")) {
          inv(commandReader);
        }
        else if (commandReader.isCommandStartsWith("des")) {
          des(commandReader);
        }
        else if (commandReader.isCommandStartsWith("lsAttrs")) {
  	      attr(commandReader);
        }
        else if (commandReader.isCommand("ls")) {
          ls(commandReader);
        }
        else if (commandReader.isCommandStartsWith("mkrgn")) {
          mkrgn(commandReader);
        }
        else if (commandReader.isCommandStartsWith("chrgn")) {
          chrgn(commandReader);
        }
        else if (commandReader.isCommandStartsWith("load")) {
          load(commandReader);
        }
        else if (commandReader.isCommandStartsWith("exec")) {
          exec(commandReader);
        }
        else if (commandReader.isCommandStartsWith("query")) {
          query(commandReader);
        }
        else if (commandReader.isCommandStartsWithNoCase("existsValue")) {
          existsValue(commandReader);
        }
        else if (commandReader.isCommandStartsWithNoCase("selectValue")) {
          selectValue(commandReader);
        }
        else if (commandReader.isCommandStartsWith("help") ||
          commandReader.isCommandStartsWith("?"))
        {
          showHelp();
        }
        else if (commandReader.isCommand("lsrgn"))
        {
          cacheInfo();
        }
        else if (commandReader.getCommandString().size() != 0) {
          printf("Unrecognized command. Enter 'help' or '?' to get a list of commands.\n");
        }
    }
    catch (Exception& ex) {
      fprintf(stderr, "Exception in CacheRunner [%s]\n", ex.getMessage());
    }
  }
}

// ----------------------------------------------------------------------------
// ************ Command implementation methods ****************************
// ----------------------------------------------------------------------------
void CacheRunner::cacheInfo()
{
  VectorOfRegion vrp;
  m_cachePtr->rootRegions(vrp);
  printf("\nNumber of regions in Cache: %d\n", vrp.size());
  int count = 1;
  for (int rgnCnt = 0; rgnCnt < vrp.size(); rgnCnt++) {
    printf("Region Name %d: %s\n", count++, vrp.at(rgnCnt)->getName());
  }

}
// ----------------------------------------------------------------------------
/**
  * Creates a new region
  * @see RegionFactory#create
  */
// ----------------------------------------------------------------------------

void CacheRunner::mkrgn(CommandReader& commandReader) throw ( Exception )
{
  std::string sName = commandReader.getTokenString(1); // read second token from command string

  if (sName.size() > 0) {
    PoolFactoryPtr poolFactPtr = PoolManager::createFactory();
    poolFactPtr->addServer("localhost", 50505);
    if((PoolManager::find("examplePool"))== NULLPTR ) {// Pool does not exist with the same name.
      PoolPtr pptr = poolFactPtr->create("examplePool");
    }
    RegionFactoryPtr regionFactory = m_cachePtr->createRegionFactory(CACHING_PROXY);
    RegionPtr regionPtr = regionFactory
         ->setCachingEnabled(true)
         ->setPoolName("examplePool")
         ->create(sName.c_str());
  }
}

//----------------------------------------------------------------------------
void CacheRunner::exec(CommandReader& commandReader) throw ( Exception )
{
  std::string sName = commandReader.getTokenString(1,true);
  printf(" query string is %s\n",sName.c_str());
  try {
    QueryServicePtr qs = m_cachePtr->getQueryService("examplePool");
    QueryPtr q = qs->newQuery(sName.c_str());
    SelectResultsPtr sptr = q->execute();
    SelectResultsIterator iptr = sptr->getIterator();
    ResultSetPtr rsptr;
    StructSetPtr ssptr;
    StructPtr siptr;
    CacheableStringPtr cStrptr;
    printf("Query results : Found %d row \n",sptr->size());
    for( int32_t rows = 0; rows < sptr->size(); rows++)
    {
      SerializablePtr tmps = (*sptr)[rows];
      if (instanceOf<StructPtr>(tmps)) {
        siptr = staticCast<StructPtr>(tmps);
        printf("Struct with %d fields \n",siptr->length());
        for(int32_t cols = 0; cols < siptr->length(); cols++)
        {
          SerializablePtr field = (*siptr)[cols];
          if (field == NULLPTR) {
            printf("we got null fields here, probably we have NULL data\n");
            continue;
          }
          printStructSet(field, siptr, cols);
        }
      }
      else{
          printResultset(tmps);
      }
    }
  } catch(Exception& ex)
  {
    printf("Exception in CacheRunner::exec [%s]\n", ex.getMessage());
  }
}

//----------------------------------------------------------------------------
void CacheRunner::query(CommandReader& commandReader) throw ( Exception )
{
  std::string sName = commandReader.getTokenString(1,true);
  printf(" query predicate is %s\n",sName.c_str());
  try {
    SelectResultsPtr sptr = m_currRegionPtr->query(sName.c_str(), 123);
    SelectResultsIterator iptr = sptr->getIterator();
    ResultSetPtr rsptr;
    StructSetPtr ssptr;
    StructPtr siptr;
    printf("Query results : Found %d row \n",sptr->size());
    for( int32_t rows = 0; rows < sptr->size(); rows++)
    {
      SerializablePtr tmps = (*sptr)[rows];
      if (instanceOf<StructPtr>(tmps)) {
        siptr = staticCast<StructPtr>(tmps);
        printf("Struct with %d fields \n",siptr->length());
        for(int32_t cols = 0; cols < siptr->length(); cols++)
        {
          SerializablePtr field = (*siptr)[cols];
          if (field == NULLPTR) {
            printf("we got null fields here, probably we have NULL data\n");
            continue;
          }
          printStructSet(field, siptr, cols);
        }
      }
      else{
          printResultset(tmps);
      }
    }
  } catch(Exception& ex)
  {
    printf("Exception in CacheRunner::query [%s]\n", ex.getMessage());
  }
}

//----------------------------------------------------------------------------
void CacheRunner::existsValue(CommandReader& commandReader) throw ( Exception )
{
  std::string sName = commandReader.getTokenString(1,true);
  printf(" query predicate is %s\n",sName.c_str());
  try {
    bool result = m_currRegionPtr->existsValue(sName.c_str());
    if (result)
    {
      printf("Query result is TRUE\n");
    }
    else
    {
      printf("Query result is FALSE\n");
    }
  } catch(Exception& ex)
  {
    printf("Exception in CacheRunner::existsValue [%s]\n", ex.getMessage());
  }
}

//----------------------------------------------------------------------------
void CacheRunner::selectValue(CommandReader& commandReader) throw ( Exception )
{
  std::string sName = commandReader.getTokenString(1,true);
  printf(" query predicate is %s\n",sName.c_str());
  try {
    SerializablePtr sptr = m_currRegionPtr->selectValue(sName.c_str());
    ResultSetPtr rsptr;
    StructSetPtr ssptr;
    StructPtr siptr;
    if (instanceOf<StructPtr>(sptr)) {
      siptr = staticCast<StructPtr>(sptr);
      printf("Struct with %d fields \n",siptr->length());
      for(int32_t cols = 0; cols < siptr->length(); cols++)
      {
        SerializablePtr field = (*siptr)[cols];
        if (field == NULLPTR) {
          printf("we got null fields here, probably we have NULL data\n");
          continue;
        }
        printStructSet(field, siptr, cols);
      }
    }
    else{
        printResultset(sptr);
    }
  } catch(Exception& ex)
  {
    printf("Exception in CacheRunner::selectValue [%s]\n", ex.getMessage());
  }
}

void CacheRunner::printStructSet(CacheablePtr field, StructPtr ssptr,
		int32_t& fields)
{
  CacheableStringArrayPtr strArr = NULLPTR;
  CacheableHashMapPtr map = NULLPTR;
  StructPtr structimpl = NULLPTR;

  if (field != NULLPTR) {
    if (instanceOf<CacheableStringArrayPtr> (field)) {
      strArr = staticCast<CacheableStringArrayPtr> (field);
      for (int stri = 0; stri < strArr->length(); stri++)
        printf("%s(%d) - %s \n", ssptr->getFieldName(fields), stri,
            strArr->operator[](stri)->asChar());
    }
    else if (instanceOf<CacheableHashMapPtr> (field)) {
      map = staticCast<CacheableHashMapPtr> (field);
      int index = 0;
      for (CacheableHashMap::Iterator iter = map->begin(); iter != map->end();
        ++iter) {
        printf("hashMap %d of %d ... \n", ++index, map->size());
        printStructSet(iter.first(), ssptr, fields);
        printStructSet(iter.second(), ssptr, fields);
      }
      printf("end of map \n");
    }
    else if (instanceOf<StructPtr> (field)) {
      structimpl = staticCast<StructPtr> (field);
      printf("structImpl %s {\n", ssptr->getFieldName(fields));
      for (int32_t inner_fields = 0; inner_fields < structimpl->length();
        ++inner_fields) {
        SerializablePtr field = (*structimpl)[inner_fields];
        if (field == NULLPTR) {
          printf("we got null fields here, probably we have NULL data\n");
          continue;
        }

        printStructSet(field, structimpl, inner_fields);

      } //end of field iterations
      printf("   } //end of %s\n", ssptr->getFieldName(fields));
    }
    else
      printf("%s : %s\n", ssptr->getFieldName(fields),
          field->toString()->asChar());
  }
  else {
    printf("unknown field data.. couldn't even convert it to "
      "Cacheable variants\n");
  }
}

void CacheRunner::printResultset(SerializablePtr field)
{
  CacheableStringArrayPtr strArr;
  CacheableHashMapPtr map;
  if (field != NULLPTR) {
    if (instanceOf<CacheableStringArrayPtr> (field)) {
      strArr = staticCast<CacheableStringArrayPtr> (field);
      for (int stri = 0; stri < strArr->length(); ++stri)
        printf("(%d) - %s \n", stri, strArr->operator[](stri)->asChar());
    }
    else if (instanceOf<CacheableHashMapPtr> (field)) {
      map = staticCast<CacheableHashMapPtr> (field);
      int index = 0;
      for (CacheableHashMap::Iterator iter = map->begin(); iter != map->end();
        ++iter) {
        printf("hashMap %d of %d ... \n", ++index, map->size());
        printResultset(field);
        printResultset(field);
      }
      printf("end of map \n");
    }
    else {
      printf("%s\n", field->toString()->asChar());
    }
  }
  else {
    printf("unknown field data.. couldn't even convert it to "
      "Cacheable variants\n");
  }
}

// ----------------------------------------------------------------------------
/**
  * Changes the current region to another as specified by
  * <code>command</code>.
  *
  * @see Cache#getRegion
  * @see Region#getSubregion
  * @see Region#getParentRegion
  */
// ----------------------------------------------------------------------------

void CacheRunner::chrgn(CommandReader& commandReader) throw ( Exception )
{
  VectorOfRegion vrp;
  if (commandReader.getNumberOfTokens() == 1) { // if only the command token exists
    m_cachePtr->rootRegions(vrp);
    RegionPtr regPtr1 = vrp.at(vrp.size()-1);

    m_currRegionPtr = m_cachePtr->getRegion(vrp.at(vrp.size()-1)->getName());
    return;
  }
  m_cachePtr->rootRegions(vrp);
  std::string sName = commandReader.getTokenString(1); // read second token from command string

  if (sName.size() == 0)
    return;

  RegionPtr tmpRgnPtr;

  tmpRgnPtr = m_cachePtr->getRegion(sName.c_str());

  if (tmpRgnPtr != NULLPTR) {
    m_currRegionPtr = tmpRgnPtr;
  }
  else {
    printf("Region %s not found\n", sName.c_str());
  }
}

// ----------------------------------------------------------------------------
/**
  * Invalidates (either local or distributed) a region or region
  * entry depending on the contents of <code>command</code>.
  *
  * @see Region#invalidateRegion
  * @see Region#invalidate
  */
// ----------------------------------------------------------------------------

void CacheRunner::inv(CommandReader& commandReader) throw ( Exception )
{
  std::string arg1;
  std::string arg2;
  bool inv_l = false;

  switch (commandReader.getNumberOfTokens()) {
    case 1:
      // inv followed by nothing invalidates the current region
      m_currRegionPtr->invalidateRegion();
      break;
    case 2:
      arg1 = commandReader.getTokenString(1);
      inv_l = (arg1 == "-l") ? true : false;
      if (inv_l) {
        // inv -l local invalidates current region
        m_currRegionPtr->localInvalidateRegion();
      }
      else {
        // inv name invalidate the entry name in current region
        m_currRegionPtr->invalidate(createKey( arg1.c_str() ));

      }
      break;
    case 3:
      // inv -l name local invalidates name in current region
      arg1 = commandReader.getTokenString(1);
      arg2 = commandReader.getTokenString(2);
      inv_l = (arg1 == "-l") ? true : false;
      if (inv_l) {
        m_currRegionPtr->localInvalidate(createKey( arg2.c_str() ));
      }
      break;
    default:
      break;
  }
}

// ----------------------------------------------------------------------------
/**
  * Resets the current region attributes
  */
// ----------------------------------------------------------------------------

void CacheRunner::reset() throw ( Exception )
{
  /** @todo could not find previously saved settings */
  AttributesFactory* pAttributeFactory = new AttributesFactory( );
  assert(pAttributeFactory != NULL);
  if (pAttributeFactory){
    m_currRegionAttributesPtr = pAttributeFactory->createRegionAttributes();
    printf("attributes have been reset to defaults\n");

    _GF_SAFE_DELETE(pAttributeFactory);
  }
}


/**
  * Gets a cached object from the current region and prints out its
  * <code>String</code> value.
  *
  * @see Region#get
  */
// ----------------------------------------------------------------------------

void CacheRunner::get(CommandReader& commandReader) throw (Exception)
{
  std::string sKey = commandReader.getTokenString(1); // read second token from command string

  if (sKey.size() != 0) {
    std::string sValue;
    try {
      CacheablePtr cacheablePtr = m_currRegionPtr->get(sKey.c_str());
      CacheableKeyPtr sKeyPtr = CacheableString::create( sKey.c_str());

      printEntry(sKeyPtr, cacheablePtr);
    } catch ( const Exception& ex ) {
      printf("Entry not found\n");
    }

  }
}

/**
  * run a certain number of get() on the specified entry
  *
  * @see Region#get
  */
// ----------------------------------------------------------------------------

int getUsedTime(struct timeval tv1, struct timeval tv2) {
    int sec = tv2.tv_sec - tv1.tv_sec;
    int usec = tv2.tv_usec - tv1.tv_usec;
    if (usec < 0) {
      sec--;
      usec = 1000000-usec;
    }
    return(sec*1000000+usec);
}

void CacheRunner::run(CommandReader& commandReader) throw (Exception)
{
  int iTokenSize = commandReader.getNumberOfTokens();
  std::string sValue = "null";
  if (iTokenSize < 3 ) {
    printf("Usage:run numberOfOp sizeOfData\n");
    return;
  }

  sValue = commandReader.getTokenString(1);
  int number = atoi(sValue.c_str());
  sValue = commandReader.getTokenString(2);
  int size = atoi(sValue.c_str());

  // VectorOfCacheableKey keys;
  std::string val(size,'a');
  CacheablePtr value = CacheableBytes::create(( const unsigned char * )val.c_str(), val.length());

  // for (int i=0; i<number; i++) {
    // keys.push_back(CacheableKey::create( i ));
  // }

    // std::string val(10,'a');
    HashMapOfCacheable map;
    map.clear();
#if defined(WIN32)
    clock_t tv1 = clock();
#else
    struct timeval tv1;
    gettimeofday(&tv1, NULL);
#endif
    for (int i=0; i<number; i++) {
      CacheableKeyPtr keyPtr = CacheableKey::create( i );
      if (size == 0) {
        ExampleObjectPtr newObj(new ExampleObject(i));
        map.insert(keyPtr, newObj);
      } else {
        CacheableBytesPtr valuePtr = CacheableBytes::create(
            (const unsigned char *)val.c_str(), val.length());
        map.insert(keyPtr, valuePtr);
      }
    }
#if defined(WIN32)
    clock_t tv2 = clock();
#else
    struct timeval tv2;
    gettimeofday(&tv2, NULL);
#endif
    m_currRegionPtr->putAll(map);

#if defined(WIN32)
    clock_t tv3 = clock();
    printf("prepare:%f, run:%f, %f\n", (tv2-tv1)*1.0/CLOCKS_PER_SEC, (tv3-tv2)*1.0/CLOCKS_PER_SEC, number*1.0*CLOCKS_PER_SEC/(tv3-tv2));
#else
    struct timeval tv3;
    gettimeofday(&tv3, NULL);
    int prep_t = getUsedTime(tv1, tv2);
    int run_t = getUsedTime(tv1, tv3);
    double run_tf = run_t/1000000.0;
    printf("prepare:%d.%d, run:%d.%d, %f\n", prep_t/1000000, prep_t%1000000, run_t/1000000, run_t%1000000, number/run_tf);
#endif

#if 0
  {
    for (int i=0; i<number; i++) {
      try {
        m_currRegionPtr->put(keys[i], value);
      } catch ( const Exception& ex ) {
        printf("failed to put entry %d %s", i, ex.getMessage());
      }
    }
    printf("Put: OpNum:%d, dataSize:%d\n", number, size);
  }

  {
    for (int i=0; i<number; i++) {
      try {
        m_currRegionPtr->localInvalidate(keys[i]);
      } catch ( const Exception& ex ) {
        printf("localInvalidate failed\n");
        return;
      }
    }
  }

  {
    CacheablePtr valuePtr;
    for (int i=0; i<number; i++) {
      try {
        valuePtr = m_currRegionPtr->get(keys[i]);
      } catch ( const Exception& ex ) {
        printf("Entry not found\n");
        return;
      }
    }
    printf("Get: OpNum:%d, dataSize:%d\n", number, size);
  }

  {
    for (int i=0; i<number; i++) {
      try {
        m_currRegionPtr->localInvalidate(keys[i]);
      } catch ( const Exception& ex ) {
        printf("localInvalidate failed\n");
      }
    }
  }

  {
    for (int i=0; i<number; i++) {
      try {
        m_currRegionPtr->destroy(keys[i]);
      } catch ( const Exception& ex ) {
        printf("failed to destroy entry %d %s", i, ex.getMessage());
      }
    }
    printf("Destroy: OpNum:%d, dataSize:%d\n", number, size);
  }
#endif
}

/**
  * register interested keys
  *
  * @see Region#registerKeys
  */
// ----------------------------------------------------------------------------

void CacheRunner::registerKeys(CommandReader& commandReader) throw (Exception)
{
  int iTokenSize = commandReader.getNumberOfTokens();
  std::string sValue = "null";
  if (iTokenSize < 2 ) {
    printf("Usage:reg k1 k2 ... kn\n");
    return;
  }

  VectorOfCacheableKey keys;
  for (int i=1; i<iTokenSize; i++)
  {
    sValue = commandReader.getTokenString(i);
    keys.push_back(CacheableString::create( sValue.c_str() ));
  }

  try {
    m_currRegionPtr->registerKeys(keys);
  } catch ( const Exception& ex ) {
    printf("failed to register keys %s", ex.getMessage());
  }
}

/**
  * register regular expression
  *
  * @see Region#registerRegex
  */
// ----------------------------------------------------------------------------

void CacheRunner::registerRegex(CommandReader& commandReader) throw (Exception)
{
  int iTokenSize = commandReader.getNumberOfTokens();
  std::string sValue = "null";
  if (iTokenSize < 2 || iTokenSize > 2) {
    printf("Usage:regex k.* or k-[2-3] ...Enter a regular expression string \n");
    return;
  }


  try {
    sValue = commandReader.getTokenString(1);
    m_currRegionPtr->registerRegex(sValue.c_str());
  } catch ( const Exception& ex ) {
    printf("failed to register regular expression %s\n", ex.getMessage());
  }
}

/**
  * unregister interested keys
  *
  * @see Region#unregisterKeys
  */
// ----------------------------------------------------------------------------

void CacheRunner::unregisterKeys(CommandReader& commandReader) throw (Exception)
{
  int iTokenSize = commandReader.getNumberOfTokens();
  std::string sValue = "null";
  if (iTokenSize < 2 ) {
    printf("Usage:unreg k1 k2 ... kn\n");
    return;
  }

  VectorOfCacheableKey keys;
  for (int i=1; i<iTokenSize; i++)
  {
    sValue = commandReader.getTokenString(i);
    keys.push_back(CacheableString::create( sValue.c_str() ));
  }

  try {
    m_currRegionPtr->unregisterKeys(keys);
  } catch ( const Exception& ex ) {
    printf("failed to unregister keys %s", ex.getMessage());
  }
}

/**
  * unregister regular expression
  *
  * @see Region#unregisterRegex
  */
// ----------------------------------------------------------------------------

void CacheRunner::unregisterRegex(CommandReader& commandReader) throw (Exception)
{
  int iTokenSize = commandReader.getNumberOfTokens();
  std::string sValue = "null";
  if (iTokenSize < 2 ) {
    printf("Usage:unregex k.* or k-[2-3] ...Enter a regular expression string \n");
    return;
  }

  try {
    sValue = commandReader.getTokenString(1);
    m_currRegionPtr->unregisterRegex(sValue.c_str());
  } catch ( const Exception& ex ) {
    printf("failed to unregister regular expression %s\n", ex.getMessage());
  }
}
//----------------------------------------------------------------------------
/**
  * Creates a new entry in the current region
  *
  * @see Region#create
  */
// ----------------------------------------------------------------------------

void CacheRunner::create(CommandReader& commandReader) throw ( Exception )
{
  int iTokenSize = commandReader.getNumberOfTokens();

  if (iTokenSize < 2 ) {
    printf("Error:create requires a name \n");
  }
  else {
    std::string sKey = commandReader.getTokenString(1); // read second token from command string
    if (iTokenSize > 2) {
      std::string sValue = commandReader.getTokenString(2);
      if (iTokenSize > 3) {
        if (commandReader.isTokenNoCase("int", 3)){
          /** @todo Could not find create for a integer */
 	  CacheableKeyPtr keyPtr = CacheableKey::create(sKey.c_str());
          CacheableInt32Ptr ValuePtr = CacheableInt32::create( atoi(sValue.c_str()));
          m_currRegionPtr->create(keyPtr,ValuePtr);
        }
        else if (commandReader.isTokenNoCase("long", 3)){
          /** @todo Could not find create for a long long */
 	  CacheableKeyPtr keyPtr = CacheableKey::create(sKey.c_str());
          CacheableInt64Ptr ValuePtr = CacheableInt64::create( atoll(sValue.c_str()));
          m_currRegionPtr->create(keyPtr,ValuePtr);
        }
        else if (commandReader.isTokenNoCase("float", 3)){
 	  CacheableKeyPtr keyPtr = CacheableKey::create(sKey.c_str());
	  CacheableFloatPtr ValuePtr = CacheableFloat::create(atof(sValue.c_str()));
          m_currRegionPtr->create(keyPtr,ValuePtr);
        }
        else if (commandReader.isTokenNoCase("double", 3)){
 	  CacheableKeyPtr keyPtr = CacheableKey::create(sKey.c_str());
	  CacheableDoublePtr ValuePtr = CacheableDouble::create(strtod(sValue.c_str(), NULL));
          m_currRegionPtr->create(keyPtr,ValuePtr);
        }
        else if (commandReader.isTokenNoCase("str", 3)) {
          CacheableKeyPtr keyPtr = CacheableKey::create(sKey.c_str());
          CacheableStringPtr ValuePtr = CacheableString::create( sValue.c_str());
          m_currRegionPtr->create(keyPtr, ValuePtr);
        }
	else if (commandReader.isTokenNoCase("obj", 3)) {
          ExampleObjectPtr newObj(new ExampleObject(sValue));
          CacheableKeyPtr keyPtr = CacheableKey::create(sKey.c_str());
          m_currRegionPtr->put(keyPtr, newObj);
        }
        else if (commandReader.isTokenNoCase("usr", 3)) {
          UserPtr newObj(new User(sValue.c_str(), ','));
          CacheableKeyPtr keyPtr = CacheableKey::create(sKey.c_str());
          m_currRegionPtr->put(keyPtr, newObj);
        }
        else if (commandReader.isTokenNoCase("portfolio",3)) {
          PortfolioPtr newObj(new Portfolio(atoi(sValue.c_str()), 2));
          CacheableKeyPtr keyPtr = CacheableKey::create(sKey.c_str());
          m_currRegionPtr->put(keyPtr, newObj);
        }
      else if (commandReader.isTokenNoCase("position",3)) {
          PositionPtr newObj(new Position(sValue.c_str(),atoi(sValue.c_str())));
          CacheableKeyPtr keyPtr = CacheableKey::create(sKey.c_str());
          m_currRegionPtr->put(keyPtr, newObj);
      }
        else {
          printf("Invalid object type specified. Please see help.\n");
        }
      }
      else {
      	CacheableKeyPtr keyPtr = CacheableKey::create(sKey.c_str());
	CacheableBytesPtr valuePtr = CacheableBytes::create(
	    (uint8_t *)sValue.c_str(),sValue.size());
        m_currRegionPtr->create(keyPtr, valuePtr);
      }
    }
    else {
      CacheableKeyPtr keyPtr = CacheableKey::create(sKey.c_str());
      CacheablePtr valuePtr(NULLPTR);
      m_currRegionPtr->create(keyPtr, valuePtr);

    }
  }
}

// ----------------------------------------------------------------------------
/**
  * Puts an entry into the current region
  *
  * @see Region#put
  */
// ----------------------------------------------------------------------------

void CacheRunner::put(CommandReader& commandReader) throw ( Exception )
{
  int iTokenSize = commandReader.getNumberOfTokens();
  std::string sValue = "null";
  if (iTokenSize < 3 ) {
    printf("Error:put requires a name and a value\n");
  }
  else {
    std::string sKey = commandReader.getTokenString(1); // read second token from command string
    sValue = commandReader.getTokenString(2);
    if (iTokenSize > 3) {
      if (commandReader.isTokenNoCase("int", 3)) {
        /** put for integer values */
	CacheableKeyPtr keyPtr = CacheableKey::create(sKey.c_str());
	CacheableInt32Ptr ValuePtr = CacheableInt32::create(atoi(sValue.c_str()));
        m_currRegionPtr->put(keyPtr,ValuePtr);
      }
      else if (commandReader.isTokenNoCase("long", 3)) {
        /** put for long integer values */
	CacheableKeyPtr keyPtr = CacheableKey::create(sKey.c_str());
	CacheableInt64Ptr ValuePtr = CacheableInt64::create(atoll(sValue.c_str()));
        m_currRegionPtr->put(keyPtr,ValuePtr);
      }
      else if (commandReader.isTokenNoCase("float", 3)) {
        /** put for float values */
	CacheableKeyPtr keyPtr = CacheableKey::create(sKey.c_str());
	CacheableFloatPtr ValuePtr = CacheableFloat::create(atof(sValue.c_str()));
        m_currRegionPtr->put(keyPtr,ValuePtr);
      }
      else if (commandReader.isTokenNoCase("double", 3)) {
        /** put for double values */
	CacheableKeyPtr keyPtr = CacheableKey::create(sKey.c_str());
	CacheableDoublePtr ValuePtr = CacheableDouble::create(strtod(sValue.c_str(), NULL));
        m_currRegionPtr->put(keyPtr,ValuePtr);
      }
      else if (commandReader.isTokenNoCase("date", 3)) {
        /** put for date values */
	CacheableKeyPtr keyPtr = CacheableKey::create(sKey.c_str());
        struct tm tm_dt = { 0 };
        tm_dt.tm_isdst = -1; //determine by itself.
        time_t epochtime = 0;
#ifdef WIN32
        int yr=0, mon=0, dy=0;
        if( sscanf(sValue.c_str(), "%d-%d-%d%*s", &yr, &mon, &dy) != 3) {
          printf("invalid date format - date should be ISO format yyyy-mm-dd\n");
          return;
        }
        tm_dt.tm_year=yr-1900;
        tm_dt.tm_mon=mon-1;
        tm_dt.tm_mday=dy;
        epochtime = mktime(&tm_dt);
#elif !defined(WIN32)
        if(strptime(sValue.c_str(), "%Y-%m-%d", &tm_dt) == NULL) {
          printf("invalid date format - date should be ISO format yyyy-mm-dd\n");
          return;
        }
        epochtime = mktime( &tm_dt );
#endif
        if(epochtime == -1) { printf("ERROR: epoch time could not be computed.\n"); }
	CacheableDatePtr ValuePtr = CacheableDate::create(epochtime);
        m_currRegionPtr->put(keyPtr,ValuePtr);
      }
      else if (commandReader.isTokenNoCase("str", 3)) {
        CacheableKeyPtr keyPtr = CacheableKey::create(sKey.c_str());
        CacheableStringPtr ValuePtr = CacheableString::create( sValue.c_str());
        m_currRegionPtr->put(keyPtr, ValuePtr);
      }
      else if (commandReader.isTokenNoCase("obj", 3)) {
	// ExampleObjectPtr newObj = new ExampleObject(atoi(sValue.c_str()),(atoi(sValue.c_str()))*4);
        ExampleObjectPtr newObj(new ExampleObject(sValue));
	CacheableKeyPtr keyPtr = CacheableKey::create(sKey.c_str());
	m_currRegionPtr->put(keyPtr, newObj);
      }
      else if (commandReader.isTokenNoCase("usr", 3)) {
        // UserPtr newObj = new User(name_str, atoi(userId_str.c_str()));
        UserPtr newObj(new User(sValue, atoi(sValue.c_str())));
        CacheableKeyPtr keyPtr = CacheableKey::create(sKey.c_str());
        m_currRegionPtr->put(keyPtr, newObj);
      }
      else if (commandReader.isTokenNoCase("portfolio",3)) {
          PortfolioPtr newObj(new Portfolio(atoi(sValue.c_str()), 2));
          CacheableKeyPtr keyPtr = CacheableKey::create(sKey.c_str());
          m_currRegionPtr->put(keyPtr, newObj);
      }
      else if (commandReader.isTokenNoCase("position",3)) {
          PositionPtr newObj(new Position(sValue.c_str(),atoi(sValue.c_str())));
          CacheableKeyPtr keyPtr = CacheableKey::create(sKey.c_str());
          m_currRegionPtr->put(keyPtr, newObj);
      }
      else {
        printf("Invalid object type specified. Please see help.\n");
      }
    }
    else {
      /**  put for bytes */
      CacheableKeyPtr keyPtr = CacheableKey::create(sKey.c_str());
      CacheableBytesPtr ValuePtr = CacheableBytes::create((uint8_t*)sValue.c_str(),sValue.size());
      m_currRegionPtr->put(keyPtr,ValuePtr);
    }
  }
}

void CacheRunner::putAll(CommandReader& commandReader) throw ( Exception )
{
  std::string sValue = "null";
    std::string sKey = commandReader.getTokenString(1); // read second token from command string
    sValue = commandReader.getTokenString(2);
    int size = atoi(sValue.c_str());
    char buf[20];
    std::string val(10,'a');
    HashMapOfCacheable map;
    map.clear();
    for (int i=0; i<size; i++) {
      sprintf(buf,"%s%d", sKey.c_str(), i);
      CacheableKeyPtr keyPtr = CacheableKey::create(buf);
      CacheableBytesPtr valuePtr = CacheableBytes::create(( const unsigned char * )val.c_str(), val.length());
      map.insert(keyPtr, valuePtr);
      // ExampleObjectPtr newObj = new ExampleObject(i);
      // map.insert(keyPtr, newObj);
    }
    for ( HashMapOfCacheable::Iterator iter = map.begin( ); iter != map.end( ); ++iter ) {
      CacheableKeyPtr key = iter.first();
      CacheablePtr value = iter.second();
printf("%s, %s\n", key->toString()->asChar(), value->toString()->asChar());
    }
    m_currRegionPtr->putAll(map, 35);
}

// ----------------------------------------------------------------------------
/**
  * Destroys (local or distributed) a region or entry in the current
  * region.
  *
  * @see Region#destroyRegion
  */
// ----------------------------------------------------------------------------

void CacheRunner::des(CommandReader& commandReader) throw ( Exception )
{
  std::string arg1;
  std::string arg2;
  bool des_l= false;

  switch (commandReader.getNumberOfTokens()) {
    case 1:
      // inv followed by nothing invalidates the current region
      m_currRegionPtr->destroyRegion();
      break;
    case 2:
      arg1 = commandReader.getTokenString(1); // read second token from command string
      des_l= (arg1 == "-l") ? true : false;

      if (des_l) {
        // inv -l local invalidates current region
        m_currRegionPtr->localDestroyRegion();
      }
      else {
        // inv name invalidate the entry name in current region
        m_currRegionPtr->destroy(createKey( arg1.c_str() ));
      }
      break;
    case 3:
      // inv -l name local invalidates name in current region
      arg1 = commandReader.getTokenString(1); // read second token from command string
      arg2 = commandReader.getTokenString(2); // read third token from command string
      des_l = (arg1 == "-l") ? true : false;

      if (des_l) {
        m_currRegionPtr->localDestroy(createKey( arg2.c_str() ));
      }
      break;
    default:
      break;
  }
}

// ----------------------------------------------------------------------------
/**
  * Lists the contents of the current region.
  *
  * @see Region#entries
  */
// ----------------------------------------------------------------------------
void  CacheRunner::printAttribute(RegionAttributesPtr& attr)
{
  std::string scope("null");
  int option = attr->getScope();
  switch( option) {
    case 0:
      scope = "LOCAL";
      break;
    case 1:
      scope = "DISTRIBUTED_NO_ACK";
      break;
    case 2:
      scope = "DISTRIBUTED_ACK";
      break;
    case 3:
      scope = "GLOBAL";
      break;
    case 4:
      scope = "INVALID";
      break;

  }
  printf("Scope: %s\n",scope.c_str());
  printf("CachingEnable: %s\n",attr->getCachingEnabled() ?"enabled" : "disabled");
  printf("InitialCapacity: %d\n",attr->getInitialCapacity());
  printf("LoadFactor: %f\n",attr->getLoadFactor());
  printf("ConcurencyLevel: %d\n",attr->getConcurrencyLevel());
  printf("RegionTimeToLive: %d\n",attr->getRegionTimeToLive());
  printf("RegionIdleTimeout: %d\n",attr->getRegionIdleTimeout());
  printf("EntryTimeToLive: %d\n",attr->getEntryTimeToLive());
  printf("EntryIdleTimeout: %d\n",attr->getEntryIdleTimeout());
  printf("getLruEntriesLimit: %d\n",attr->getLruEntriesLimit());

}

void CacheRunner::attr(CommandReader& commandReader) throw ( Exception )
{
  RegionAttributesPtr regionAttributePtr = m_currRegionPtr->getAttributes();

  if (regionAttributePtr == NULLPTR) {
    printf("region attributes: RegionNotFound\n");
  }
  else {
    printAttribute(regionAttributePtr);
  }
}

void CacheRunner::ls(CommandReader& commandReader) throw ( Exception )
{
  bool ls_l = commandReader.isToken("-l", 1) ? true : false;
  if (ls_l) {
    printf("%s attributes:\n", m_currRegionPtr->getFullPath());
    m_currRegionAttributesPtr = m_currRegionPtr->getAttributes();
    printAttribute(m_currRegionAttributesPtr);
  }

  printf("Region Entries:\n");

  VectorOfRegionEntry regionEntryVector;
  m_currRegionPtr->entries(regionEntryVector, false);  // do not recurse

  RegionEntryPtr  regionEntryPtr;
  CacheableKeyPtr cacheableKeyPtr;
  CacheablePtr    cacheablePtr;

  for (int32_t ulIndex = 0; ulIndex < regionEntryVector.size(); ulIndex++){
    regionEntryPtr = regionEntryVector.at(ulIndex);
    cacheableKeyPtr = regionEntryPtr->getKey();
    cacheablePtr = regionEntryPtr->getValue();

    printEntry(cacheableKeyPtr,cacheablePtr);
  }

  printf("\n");
}

// ----------------------------------------------------------------------------
/**
  * Prints the key/value pair for an entry
  * This method recognizes a subset of all possible object types.
  */
// ----------------------------------------------------------------------------

void CacheRunner::printEntry(CacheableKeyPtr& sKey, CacheablePtr& valueBytes)
{
  std::string sValue = "null";
   if (valueBytes != NULLPTR) {
     int8_t typeId = valueBytes->typeId();
     std::string objType;
     if (typeId == GemfireTypeIds::CacheableBytes) {
       objType = "Bytes: ";
       CacheableBytesPtr cBytePtr = staticCast<CacheableBytesPtr>( valueBytes );
       const uint8_t* bytType = cBytePtr->value();
       const uint32_t len = cBytePtr->length();
       char buff[1024];
       sprintf(buff,"%s",(char*)bytType);
       buff[len] = '\0';
       std::string byteType(buff);
       sValue = objType + byteType;
     }
     else {
       switch (typeId)
       {
         case GemfireTypeIds::CacheableASCIIString: objType = "String: "; break;
         case GemfireTypeIds::CacheableInt16: objType = "Int16: "; break;
         case GemfireTypeIds::CacheableInt32: objType = "Int32: "; break;
         case GemfireTypeIds::CacheableInt64: objType = "Int64: "; break;
         case GemfireTypeIds::CacheableDouble: objType = "Double: "; break;
         case GemfireTypeIds::CacheableFloat: objType = "Float: "; break;
         case GemfireTypeIds::CacheableByte: objType = "Byte: "; break;
         default: objType = ""; break;
       }
       sValue = objType + valueBytes->toString()->asChar();
     }
   }
   else {
     sValue = "No value in cache.";
   }
   printf("\n\t %s -> %s\n", sKey->toString()->asChar(), sValue.c_str());
}

// ----------------------------------------------------------------------------
/**
  * Sets an expiration attribute of the current region
  *
  * @see Region#getAttributesMutator
  */
// ----------------------------------------------------------------------------

void CacheRunner::setExpirationAttr(CommandReader& commandReader) throw ( Exception )
{
  AttributesMutatorPtr attributesMutatorPtr = m_currRegionPtr->getAttributesMutator();
  ExpirationAttributes* pExpirationAttributes = NULL;

  bool bRegionIdle = false;
  bool bEntryIdle = false;
  bool bEntryTtl = false;
  bool bRegionTtl = false;
  int  iTime = 0;

  //Item 0 is the command itself

  std::string sAttrName   = commandReader.getTokenString(2);
  std::string sAttrValue  = commandReader.getTokenString(3);
  std::string sAttrAction = commandReader.getTokenString(4);

  if (sAttrValue.size() != 0) {
    iTime  = atoi(sAttrValue.c_str());
  }

  if (commandReader.isTokenStartsWith("regionIdleTime", 2)) {
    if (sAttrValue.size() != 0)
      bRegionIdle = true;
  }
  else if (commandReader.isTokenStartsWith("entryIdleTime", 2)) {
    if (sAttrValue.size() != 0)
      bEntryIdle = true;
  }
  else if (commandReader.isTokenStartsWith("entryTTL", 2)) {
    if (sAttrValue.size() != 0)
      bEntryTtl = true;
  }
  else if (commandReader.isTokenStartsWith("regionTTL", 2)) {
    if (sAttrValue.size() != 0)
      bRegionTtl = true;
  }
  else {
    printf("Unrecognized attribute name: %s", sAttrName.c_str());
  }

  if (bRegionIdle || bEntryIdle || bEntryTtl || bRegionTtl) {
    pExpirationAttributes = parseExpAction(iTime, sAttrAction);
    if (pExpirationAttributes == NULL)
      pExpirationAttributes = new ExpirationAttributes(iTime, ExpirationAction::INVALIDATE);
  }

  if (bRegionIdle){
    attributesMutatorPtr->setRegionIdleTimeout(pExpirationAttributes->getTimeout());
    attributesMutatorPtr->setRegionIdleTimeoutAction(pExpirationAttributes->getAction());
  }
  else if (bEntryIdle){
    attributesMutatorPtr->setEntryIdleTimeout(pExpirationAttributes->getTimeout());
    attributesMutatorPtr->setEntryIdleTimeoutAction(pExpirationAttributes->getAction());
    }
  else if (bEntryTtl){
    attributesMutatorPtr->setEntryTimeToLive(pExpirationAttributes->getTimeout());
    attributesMutatorPtr->setEntryTimeToLiveAction(pExpirationAttributes->getAction());
    }
  else if(bRegionTtl){
    attributesMutatorPtr->setRegionTimeToLive(pExpirationAttributes->getTimeout());
    attributesMutatorPtr->setRegionTimeToLiveAction(pExpirationAttributes->getAction());
    }
  _GF_SAFE_DELETE(pExpirationAttributes);
}

// ----------------------------------------------------------------------------
/**
  * Sets a region attribute of the current region
  *
  * @see #setExpirationAttr
  * @see Region#getAttributesMutator
  */
// ----------------------------------------------------------------------------

void CacheRunner::setRgnAttr(CommandReader& commandReader) throw ( Exception )
{
  std::string sName = commandReader.getTokenString(1); // read second token from command string

  if (sName.size() == 0) {
    printf("set argument is not provided, Please provide proper argument\n");
    return;
  }

  if (sName == "expiration") {
    setExpirationAttr(commandReader);
    return;
  }
  else if(sName == "listener"){
    std::string sValue;
    sValue = commandReader.getTokenString(2);
    AttributesMutatorPtr attrMutator = m_currRegionPtr->getAttributesMutator();
    if(sValue == "null" || sValue == "NULL")
      attrMutator->setCacheListener(NULLPTR);
    else
      attrMutator->setCacheListener(CacheListenerPtr(new TestCacheListener()));
  }
  else {
    printf("Unrecognized attribute name: %s\n", sName.c_str());
    return;
  }

}

// ----------------------------------------------------------------------------
/**
  * Specifies the <code>cache.xml</code> file to use when creating
  * the <code>Cache</code>.  If the <code>Cache</code> has already
  * been open, then the existing one is closed.
  *
  * @see CacheFactory#create
  */
// ----------------------------------------------------------------------------

void CacheRunner::load(CommandReader& commandReader) throw ( Exception )
{
  printf("This Functionality Is Not Implemented \n");
}

// ----------------------------------------------------------------------------
/**
  * Opens the <code>Cache</code> and sets the current region to the
  * _GS_CACHE_RUNNER_REGION region.
  *
  * @see Cache#getRegion
  */
// ----------------------------------------------------------------------------

void CacheRunner::open(CommandReader& commandReader) throw ( Exception )
{
  if (connectDistributedSystem(m_sCacheXmlFileName.c_str())){
    reset();
  }
}

// ----------------------------------------------------------------------------
// ************************* Parsing methods **********************************
// ----------------------------------------------------------------------------


// ----------------------------------------------------------------------------
/**
  * Creates <code>ExpirationAttributes</code> from an expiration time
  * and the name of an expiration action.
  */
// ----------------------------------------------------------------------------

ExpirationAttributes* CacheRunner::parseExpAction( int iExpTime, std::string sActionName)
{
  ExpirationAttributes* pExpirationAttributes = NULL;

  if (CommandReader::startsWith(sActionName.c_str(), "destroy")) {
    pExpirationAttributes =
      new ExpirationAttributes(iExpTime, ExpirationAction::DESTROY);
  }
  else if (CommandReader::startsWith(sActionName.c_str(), "inv")) {
    pExpirationAttributes =
      new ExpirationAttributes(iExpTime, ExpirationAction::INVALIDATE);
  }
  else if (CommandReader::startsWith(sActionName.c_str(), "localDes")) {
    pExpirationAttributes =
      new ExpirationAttributes(iExpTime, ExpirationAction::LOCAL_DESTROY);
  }
  else if (CommandReader::startsWith(sActionName.c_str(), "localInv")) {
    pExpirationAttributes =
      new ExpirationAttributes(iExpTime, ExpirationAction::LOCAL_INVALIDATE);
  }
  else {
    printf("Expiration Action not understood: %s", sActionName.c_str());
  }
  return pExpirationAttributes;
}

// ----------------------------------------------------------------------------
/* shows user help
 */
// ----------------------------------------------------------------------------

void CacheRunner::showHelp( )
{
  printf("\nA distributed system is created with properties loaded from your gemfire.properties file.\n");
  printf("You can specify alternative property files using -DgemfirePropertyFile=path.\n\n");

  printf("load fileName - Re-initializes the cache based using a cache.xml file\n\n");

  printf("Other commands:\n\n");

  printf("ls - list all cache entries in current region and their stats.\n");
  printf("lsAttrs - list the region attributes stored in the cache.\n\n");

  printf("Entry creation and retrieval handles byte(default), String, Integer,\n");
  printf("and a complex object generated using gfgen.\n\n");
  printf("create name [value [str|int|long|float|double|obj|usr|portfolio] - define a new entry (see mkrgn create a region).\n");
  printf("The complex object fields are filled based on the value provided.\n\n");
  printf("put name value [str|int|long|float|double|date|obj|usr|portfolio|position] - associate a name with a value in the current region. Specify optionally the data type of the value \n");
  printf("   As with create, the complex object fields are filled based on the value provided.\n\n");
  printf("get name - get the value of a cache entry.\n\n");
  printf("run number size - run a certain number of get() for this size.\n\n");
  printf("putAll keyBase mapSize - putAll a map, each key is keyBase0, keyBase1, ...\n\n");
  printf("reg k1 k2 ... kn - register the interested key list.\n\n");
  printf("unreg k1 k2 ... kn - unregister the interested key list.\n\n");
  printf("regex k.* or k-[2-3] - register the regular expression string.\n\n");
  printf("unregex k.* or k-[2-3] - unregister the regular expression string.\n\n");
  printf("des [-l] [name] - destroy an object or current region.  -l is for local destroy\n");
  printf("inv [-l] [name] - invalidate a cache entry or current region.  -l is for local invalidation\n\n");

  printf("mkrgn name - create a region with the current attributes settings\n");
  printf("chrgn name - change current region (can use a local or global name)\n");
  printf("chrgn - go to cache root level.\n\n");
  printf("lsrgn - list all regions in cache.\n\n");
  printf("exec queryExpr - Execute a query. All input after the exec command is considered the query string\n");
  printf("query queryPredicate - Execute a standard query with the predicate on the current region. All input after the query command is considered the query predicate\n");
  printf("existsValue queryPredicate - Execute a standard query with the predicate on the current region and check whether any result exists. All input after the query command is considered the query predicate\n");
  printf("selectValue queryPredicate - Execute a standard query with the predicate on the current region and check for a single result item. All input after the query command is considered the query predicate\n");
  printf("set expiration  <attribute [regionIdleTime | entryIdleTime | entryTTL | regionTTL ] > <value> <action [ destroy | inv | localDes | localInv ] > \n");
  printf("set [listener] [null] - Add or remove a cache callback. Accepted values: listener. Use the optional null keyword to remove the cache callback");
  printf("Usage:set expiration regionIdleTime 100 destroy - set regionIdleTimeout to 100 second and set the expiration action as destroy\n");
  printf("\n");
  printf("help or ? - list command descriptions\n");
  printf("exit or quit: closes the current cache and exits\n\n");

  printf("You have to use mkrgn and chrgn to create and descend into a region before using entry commands\n");

  printf("\n");
}

void CacheRunner::showHelpForClientType( )
{
  printf("\nA distributed system is created with properties loaded from your gemfire.properties file.\n");
  printf("You can specify alternative property files using -DgemfirePropertyFile=path.\n\n");

  printf("load fileName - Re-initializes the cache based using a cache.xml file\n\n");

  printf("Other commands:\n\n");

  printf("ls - list all cache entries in current region and their stats.\n");

  printf("Entry creation and retrieval handles byte(default), String, Integer,\n");
  printf("and a complex object generated using gfgen.\n\n");
  printf("create name [value [str|int|obj]] - define a new entry (see mkrgn create a region).\n");
  printf("The complex object fields are filled based on the value provided.\n\n");
  printf("put name value [str|int|obj|portfolio|position] - associate a name with a value in the current region\n");
  printf("   As with create, the complex object fields are filled based on the value provided.\n\n");
  printf("get name - get the value of a cache entry.\n\n");
  printf("run number size - run a certain number of get() for this size.\n\n");
  printf("reg k1 k2 ... kn - register the interested key list.\n\n");
  printf("unreg k1 k2 ... kn - unregister the interested key list.\n\n");
  printf("regex k.* or k-[2-3] - register the regular expression string.\n\n");
  printf("unregex k.* or k-[2-3] - unregister the regular expression string.\n\n");
  printf("set [listener] [null] - Add or remove a cache callback. Accepted values: listener. Use the optional null keyword to remove the cache callback");

  printf("\n");
  printf("help or ? - list command descriptions\n");
  printf("exit or quit: closes the current cache and exits\n\n");

  printf("You have to use mkrgn and chrgn to create and descend into a region before using entry commands\n");

  printf("\n");
}

// ----------------------------------------------------------------------------
