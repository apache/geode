/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#pragma once

#include "../ICqStatusListenerN.hpp"
#include "SafeConvertN.hpp"

using namespace System;
using namespace GemStone::GemFire::Cache::Generic;
namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    { 
      namespace Generic
      {
        generic<class TKey, class TResult>
        public ref class CqStatusListenerGeneric : GemStone::GemFire::Cache::ICqStatusListener
        {
        private:

          ICqStatusListener<TKey, TResult>^ m_listener;

        public:

          virtual void AddCqListener(ICqListener<TKey, TResult>^ listener)
          {
            m_listener = dynamic_cast<ICqStatusListener<TKey, TResult>^>(listener);
          }

          virtual void OnEvent( GemStone::GemFire::Cache::CqEvent^ ev) 
          {
            //TODO:split---Done
            CqEvent<TKey, TResult> gevent(GetNativePtr<gemfire::CqEvent>(ev));
            m_listener->OnEvent(%gevent);
          }

          virtual void OnError( GemStone::GemFire::Cache::CqEvent^ ev) 
          {
            //TODO::split--Done
            CqEvent<TKey, TResult> gevent(GetNativePtr<gemfire::CqEvent>(ev));
            m_listener->OnError(%gevent);
          }

          virtual void Close() 
          {
            m_listener->Close();
          }   

          virtual void OnCqDisconnected() 
          {          
            m_listener->OnCqDisconnected();
          } 

          virtual void OnCqConnected() 
          {          
            m_listener->OnCqConnected();
          } 
        };
      }
    }
  }
}

