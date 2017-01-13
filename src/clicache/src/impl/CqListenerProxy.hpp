/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
//#include "../gf_includes.hpp"
//#include "../../../ICqListener.hpp"
//#include "../../../CqListener.hpp"
#include "../ICqListener.hpp"
#include "SafeConvert.hpp"
using namespace System;

//using namespace GemStone::GemFire::Cache;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      generic<class TKey, class TResult>
      public ref class CqListenerGeneric : GemStone::GemFire::Cache::Generic::ICqListener<Object^, Object^>
      {
        private:

          ICqListener<TKey, TResult>^ m_listener;

        public:

          virtual void AddCqListener(ICqListener<TKey, TResult>^ listener)
          {
            m_listener = listener;
          }

          virtual void OnEvent( GemStone::GemFire::Cache::Generic::CqEvent<Object^, Object^>^ ev) 
	        {
						//TODO:split---Done
            CqEvent<TKey, TResult> gevent(GetNativePtr<gemfire::CqEvent>(ev));
            m_listener->OnEvent(%gevent);
          }

          virtual void OnError(GemStone::GemFire::Cache::Generic::CqEvent<Object^, Object^>^ ev)
	        {
						//TODO::split--Done
	          CqEvent<TKey, TResult> gevent(GetNativePtr<gemfire::CqEvent>(ev));
            m_listener->OnError(%gevent);
          }
        
	        virtual void Close() 
	        {
	          m_listener->Close();
          }   
      };
    }
    }
  }
}
