/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "gf_includes.hpp"
#include "CqState.hpp"
#include <vcclr.h>

#include "impl/ManagedString.hpp"
using namespace System;
using namespace System::Runtime::InteropServices;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      String^ CqState::ToString()
      {
		  return ManagedString::Get(NativePtr->toString());
      }

      bool CqState::IsRunning()
      {
        return NativePtr->isRunning();
      }

      bool CqState::IsStopped()
      {
        return NativePtr->isStopped();
      }

      bool CqState::IsClosed()
      {
	return NativePtr->isClosed();
      }

      bool CqState::IsClosing()
      {
	return NativePtr->isClosing();
      }

      void CqState::SetState( CqStateType state )
      {
		  gemfire::CqState::StateType st =gemfire::CqState::INVALID;
		  if(state == CqStateType::STOPPED)
			  st = gemfire::CqState::STOPPED;
		  else if(state == CqStateType::RUNNING)
			  st = gemfire::CqState::RUNNING;
		  else if(state == CqStateType::CLOSED)
			  st = gemfire::CqState::CLOSED;
		  else if(state == CqStateType::CLOSING)
			  st = gemfire::CqState::CLOSING;

		  NativePtr->setState( st );
      }

      CqStateType CqState::GetState( )
      {
		gemfire::CqState::StateType st =  NativePtr->getState( );
        CqStateType state;
		if(st==gemfire::CqState::STOPPED)
			state = CqStateType::STOPPED;
		else if(st==gemfire::CqState::RUNNING)
			state = CqStateType::RUNNING;
		else if(st==gemfire::CqState::CLOSED)
			state = CqStateType::CLOSED;
		else if(st==gemfire::CqState::CLOSING)
			state = CqStateType::CLOSING;
		else
			state = CqStateType::INVALID;
		return state;
      }

    }
  }
}
 } //namespace 
