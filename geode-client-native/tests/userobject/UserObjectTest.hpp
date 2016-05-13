/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
  * @file    UserObjectTest.hpp
  * @since   1.0
  * @version 1.0
  * @see
  *
  */

// ----------------------------------------------------------------------------

#ifndef __UserObjectTest_hpp__
#define __UserObjectTest_hpp__

// ----------------------------------------------------------------------------

#include "GemfireCppCache.hpp"

#include "fwklib/FrameworkTest.hpp"
#include "fwklib/FwkLog.hpp"

#include <vector>

namespace gemfire {
namespace testframework {
  
class UserObjectTest : public FrameworkTest
{
public:

  UserObjectTest( const char * initArgs ) :
    FrameworkTest( initArgs ),
    m_bb( "UserObjectBB" ),
    m_KeysA( NULL ),
    m_EntryCount( 0 ),
    m_KeyIndexBegin( 0 )
    {}

  virtual ~UserObjectTest() { clearup(); }
  
  int32_t mput();
  int32_t mverify();
  int32_t initFromParameters();

  void checkTest( const char * taskId );

private:

  void clearup();
  RegionPtr getRegion( const char * name = NULL );
  
  std::string m_bb;
  CacheableKeyPtr * m_KeysA;
  int32_t m_EntryCount;
  int32_t m_KeyIndexBegin;
  std::vector<std::string> classList;

};

} // namespace testframework
} // namespace gemfire
// ----------------------------------------------------------------------------

#endif // __UserObjectTest_hpp__

