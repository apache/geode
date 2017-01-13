/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#define ROOT_NAME "testSystemProperties"

#include "fw_helper.hpp"
#include "gfcpp/GemfireCppCache.hpp"
#include "gfcpp/SystemProperties.hpp"
#include "gfcpp/Properties.hpp"

#ifndef WIN32
#include <unistd.h>
#endif

using namespace gemfire;

const bool checkSecurityProperties(PropertiesPtr securityProperties,
                                   const char* key, const char* value) {
  bool flag;
  if (key == NULL || value == NULL) {
    return false;
  }
  CacheableStringPtr tempValue = securityProperties->find(key);
  if (tempValue == NULLPTR) {
    return (false);
  }
  flag = strcmp(tempValue->asChar(), value);
  return (!flag);
}

BEGIN_TEST(DEFAULT)
  {
    SystemProperties* sp = new SystemProperties(NULLPTR, "./non-existent");
    ASSERT(sp->statisticsSampleInterval() == 1, "expected 1");
    ASSERT(sp->statisticsEnabled() == true, "expected true");
    LOG(sp->statisticsArchiveFile());
    const char* safname = sp->statisticsArchiveFile();
    int ret = strcmp(safname, "statArchive.gfs");
    ASSERT(ret == 0, "expected 0");
    const char* ll = Log::levelToChars(sp->logLevel());
    ret = strcmp(ll, "config");
    ASSERT(ret == 0, "expected 0");
    delete sp;
  }
END_TEST(DEFAULT)

/*
 * Commenting the last two tests because I need to know where to put
 * the file which will be passed to the constructor. These two tests works.
 * But for others it might fail so commenting them as of now.
 */
BEGIN_TEST(CONFIG)
  {
    // create a file for alternate properties...
    //  FILE* propFile = ACE_OS::fopen( "./test.properties", "a+" );
    //  ACE_OS::fprintf( propFile, "gf.transport.config=./gfconfig\n" );
    ///  ACE_OS::fprintf( propFile, "statistics.sample.interval=2000\n" );
    //  ACE_OS::fprintf( propFile, "statistics.enabled=false\n" );
    //  ACE_OS::fprintf( propFile, "statistics.archive.file=./stats.gfs\n" );
    //  ACE_OS::fprintf( propFile, "log.level=error\n" );
    //  ACE_OS::fclose( propFile );

    // Make sure product can at least log to stdout.
    Log::init(Log::Config, NULL, 0);

    SystemProperties* sp = new SystemProperties(NULLPTR, "test.properties");
    ASSERT(sp->statisticsSampleInterval() == 1, "expected 1");
    ASSERT(sp->statisticsEnabled() == true, "expected true");
    const char* safname = sp->statisticsArchiveFile();
    int ret = strcmp(safname, "statArchive.gfs");
    ASSERT(ret == 0, "expected 0");
    Log::LogLevel ll = sp->logLevel();
    ASSERT(ll == Log::Config, "expected Log::Config");
    delete sp;
  }
END_TEST(CONFIG)

BEGIN_TEST(NEW_CONFIG)
  {
    // When the tests are run from the build script the environment variable
    // TESTSRC is set.
    std::string testsrc(ACE_OS::getenv("TESTSRC"));
    std::string filePath = testsrc + "/system.properties";

    // Make sure product can at least log to stdout.
    Log::init(Log::Config, NULL, 0);

    SystemProperties* sp = new SystemProperties(NULLPTR, filePath.c_str());

    ASSERT(sp->statisticsSampleInterval() == 700, "expected 700");

    ASSERT(sp->statisticsEnabled() == false, "expected false");

    ASSERT(sp->threadPoolSize() == 96, "max-fe-thread should be 96");

    const char* safname = sp->statisticsArchiveFile();
    int ret = strcmp(safname, "stats.gfs");
    ASSERT(ret == 0, "expected 0");

    const char* logfname = sp->logFilename();
    ret = strcmp(logfname, "gfcpp.log");
    ASSERT(ret == 0, "expected 0");

    // Log::LogLevel ll = sp->logLevel();
    // ASSERT( ll == Log::Debug, "expected Log::Debug" );

    const char* name = sp->name();
    ret = strcmp(name, "system");
    ASSERT(ret == 0, "expected 0");

    const char* cxml = sp->cacheXMLFile();
    ret = strcmp(cxml, "cache.xml");
    ASSERT(ret == 0, "expected 0");

    ASSERT(sp->pingInterval() == 123, "expected 123 pingInterval");
    ASSERT(sp->redundancyMonitorInterval() == 456,
           "expected 456 redundancyMonitorInterval");

    ASSERT(sp->heapLRULimit() == 100, "expected 100");
    ASSERT(sp->heapLRUDelta() == 10, "expected 10");

    ASSERT(sp->notifyAckInterval() == 1234, "expected 1234 notifyAckInterval");
    ASSERT(sp->notifyDupCheckLife() == 4321,
           "expected 4321 notifyDupCheckLife");

    ASSERT(sp->logFileSizeLimit() == 1024000000, "expected 1024000000");

    ASSERT(sp->statsFileSizeLimit() == 1024000000, "expected 1024000000");

    const char* durableId = sp->durableClientId();
    ret = strcmp(durableId, "testDurableId");
    ASSERT(ret == 0, "expected 0");

    ASSERT(sp->durableTimeout() == 123, "expected 123 durableTimeOut");

    ASSERT(sp->connectTimeout() == 345, "expected 345 for connect timeout");

    PropertiesPtr securityProperties = sp->getSecurityProperties();
    ASSERT(checkSecurityProperties(securityProperties, "security-username",
                                   "username") == true,
           "SecurityProperties Not Stored");
    ASSERT(checkSecurityProperties(securityProperties, "security-password",
                                   "password") == true,
           "SecurityProperties Not Stored");

    delete sp;
  }
END_TEST(NEW_CONFIG)
