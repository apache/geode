/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "gf_includes.hpp"
#include "CqServiceStatistics.hpp"


namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
	uint32_t CqServiceStatistics::numCqsActive( )
	{
	  return NativePtr->numCqsActive( );
	}
    uint32_t CqServiceStatistics::numCqsCreated( )
	{
	  return NativePtr->numCqsCreated( );
	}
    uint32_t CqServiceStatistics::numCqsClosed( )
	{
	  return NativePtr->numCqsClosed( );
	}
    uint32_t CqServiceStatistics::numCqsStopped( )
	{
	  return NativePtr->numCqsStopped( );
	}
    uint32_t CqServiceStatistics::numCqsOnClient( )
	{
	  return NativePtr->numCqsOnClient( );
	}
    }
  }
}
 } //namespace 
