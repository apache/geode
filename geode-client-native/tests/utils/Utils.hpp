/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
  * @file    Utils.hpp
  * @since   1.0
  * @version 1.0
  * @see
  *
  */

// ----------------------------------------------------------------------------

#ifndef __UTILS_HPP__
#define __UTILS_HPP__

// ----------------------------------------------------------------------------
#include <GemfireCppCache.hpp>

#include "fwklib/FrameworkTest.hpp"

namespace gemfire {
namespace testframework {

// ----------------------------------------------------------------------------

class Utils : public FrameworkTest
{
public:
  Utils( const char * initArgs ) :
    FrameworkTest( initArgs ) {}

    virtual ~Utils() {}

  void checkTest( const char * taskId );
  std::string getServerSecurityParams(std::string arguments);
  int32_t doRunProcess( const char * prog, const char* args );
  int32_t doRunBgProcess( const char * prog, const char* args, int32_t waitSeconds );
  int32_t doPostSnapshot();
  int32_t doClearSnapshot();
  int32_t doValidateSnapshot();
  RegionPtr getRegion( const char * rname = NULL );
  uint8_t * hexify( const uint8_t* buff, int32_t len );
  CacheablePtr nextValue();
  CacheableKeyPtr nextKey( int32_t cnt = -1 );
  void add( CacheableKeyPtr key, CacheablePtr value );
  void add();
  void read();
  void update();
  void invalidate( bool local = false );
  void destroy( bool local = false );
  void doFeedPuts();
  int32_t doEntryOperations();
  CacheableKeyPtr getKeyNotInCache();
  CacheableKeyPtr getKeyInCache();
  std::string getSslProperty(bool server);
  std::string getKeystoreFile();
  std::string getTruststoreFile();

private:
};

} // namespace testframework
} // namespace gemfire
// ----------------------------------------------------------------------------

#endif // __UTILS_HPP__

