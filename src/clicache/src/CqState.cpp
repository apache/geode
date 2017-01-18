/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
		  apache::geode::client::CqState::StateType st =apache::geode::client::CqState::INVALID;
		  if(state == CqStateType::STOPPED)
			  st = apache::geode::client::CqState::STOPPED;
		  else if(state == CqStateType::RUNNING)
			  st = apache::geode::client::CqState::RUNNING;
		  else if(state == CqStateType::CLOSED)
			  st = apache::geode::client::CqState::CLOSED;
		  else if(state == CqStateType::CLOSING)
			  st = apache::geode::client::CqState::CLOSING;

		  NativePtr->setState( st );
      }

      CqStateType CqState::GetState( )
      {
		apache::geode::client::CqState::StateType st =  NativePtr->getState( );
        CqStateType state;
		if(st==apache::geode::client::CqState::STOPPED)
			state = CqStateType::STOPPED;
		else if(st==apache::geode::client::CqState::RUNNING)
			state = CqStateType::RUNNING;
		else if(st==apache::geode::client::CqState::CLOSED)
			state = CqStateType::CLOSED;
		else if(st==apache::geode::client::CqState::CLOSING)
			state = CqStateType::CLOSING;
		else
			state = CqStateType::INVALID;
		return state;
      }

    }
  }
}
 } //namespace 
