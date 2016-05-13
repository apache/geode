/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "ExpirationAction.hpp"
#include <string.h>

using namespace gemfire ;

char* ExpirationAction::names[]  = {(char*)"INVALIDATE",(char*)"LOCAL_INVALIDATE",(char*)"DESTROY",(char*)"LOCAL_DESTROY",(char*)NULL};

ExpirationAction::Action ExpirationAction::fromName(const char* name)
{
   return ExpirationAction::INVALIDATE;
}
    
const char*  ExpirationAction::fromOrdinal(const int ordinal)
{
   return names[ordinal];
}

ExpirationAction::ExpirationAction()
{
}
ExpirationAction::~ExpirationAction()
{
}
