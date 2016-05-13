/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "DiskPolicyType.hpp"
#include "ace/OS.h"

using namespace gemfire;

const char * DiskPolicyType::names[] = {
  "none",
  "overflows",
  "persist",
  NULL
};

const char * DiskPolicyType::fromOrdinal( const uint8_t ordinal ) 
{
   if ( ordinal > DiskPolicyType::PERSIST ) return names[DiskPolicyType::NONE];
   return names[ordinal];
}

DiskPolicyType::PolicyType DiskPolicyType::fromName( const char * name ) 
{
  uint32_t i = 0;
  while ( ( names[i] != NULL ) || ( i <= ( uint32_t )DiskPolicyType::PERSIST ) )
  {
     if( ACE_OS::strcasecmp( names[i], name ) == 0 )
     {
        return ( DiskPolicyType::PolicyType )i;
     }
     ++i;
  }
  return DiskPolicyType::NONE;
}
    
