/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
#include "ExpirationAttributes.hpp"

using namespace gemfire ;

ExpirationAttributes::ExpirationAttributes() :
  m_timeout(0),
  m_action(ExpirationAction::INVALIDATE)
{
}
  
ExpirationAttributes::ExpirationAttributes(const int expirationTime, const ExpirationAction::Action expirationAction) :
  m_timeout(expirationTime),
  m_action(ExpirationAction::INVALIDATE)
{
}
int ExpirationAttributes::getTimeout() const
{
    return m_timeout;
}
void ExpirationAttributes::setTimeout(int timeout) 
{
    m_timeout = timeout;
}
ExpirationAction::Action ExpirationAttributes::getAction() const
{
    return m_action;
}
void ExpirationAttributes::setAction(ExpirationAction::Action& action ) 
{
    m_action = action;
}
