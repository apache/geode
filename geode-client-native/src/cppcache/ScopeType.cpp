/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "ScopeType.hpp"

using namespace gemfire;

char* ScopeType::names[] = {(char*)"LOCAL", (char*)"DISTRIBUTED_NO_ACK", (char*)"DISTRIBUTED_ACK", (char*)"GLOBAL", (char*)NULL};

const char* ScopeType::fromOrdinal(const uint8_t ordinal) 
{
   if(ordinal > ScopeType::GLOBAL) return names[ScopeType::INVALID];
   return names[ordinal];
}

ScopeType::Scope ScopeType::fromName(const char* name) 
{
  uint8_t i = 0;
  while(names[i]!=NULL || i < ScopeType::INVALID)
  {
     if(!strncmp(names[i],name, strlen(names[i])))
     {
        return (ScopeType::Scope) i;
     }
     ++i;
  }
  return ScopeType::INVALID;
}
    
