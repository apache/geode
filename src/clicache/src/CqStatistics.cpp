/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "gf_includes.hpp"
#include "CqStatistics.hpp"


namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
		uint32_t CqStatistics::numInserts( )
	{
	  return NativePtr->numInserts( );
	}
    uint32_t CqStatistics::numDeletes( )
	{
	  return NativePtr->numDeletes( );
	}
    uint32_t CqStatistics::numUpdates( )
	{
	  return NativePtr->numUpdates( );
	}
    uint32_t CqStatistics::numEvents( )
	{
	  return NativePtr->numEvents( );
	}
    }
  }
}
 } //namespace 
