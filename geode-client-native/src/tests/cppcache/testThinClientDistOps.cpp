/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "ThinClientDistOps.hpp"

DUNIT_MAIN
{
  runDistOps2( false );
  runDistOps2( );

  runDistOps( false );
  runDistOps( false );
  
  runDistOps( true, true );
  runDistOps( true, true );

  runDistOps( true, false );
  runDistOps( true, false );

  runDistOps( true, true, true );
  runDistOps( true, true, true );

  runDistOps( true, false, true );
  runDistOps( true, false, true );  
}
END_MAIN
