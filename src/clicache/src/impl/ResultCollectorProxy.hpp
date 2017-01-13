/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
//#include "../../../gf_includes.hpp"
//#include "../../../IResultCollector.hpp"
//#include "../../../ResultCollector.hpp"
#include "../IResultCollector.hpp"

using namespace System;
//using namespace System::Collections::Generic;
//using namespace GemStone::GemFire::Cache;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      public interface class ResultCollectorG
      {
      public:
        void AddResult(Object^ result);
        void EndResults();
        void ClearResults();
      };

      generic<class TResult>
      public ref class ResultCollectorGeneric : ResultCollectorG
      {
        private:

          IResultCollector<TResult>^ m_rscoll;

        public:

          void SetResultCollector(IResultCollector<TResult>^ rscoll)
          {
            m_rscoll = rscoll;
          }

          virtual void AddResult( Object^ rs ) 
          {
            //gemfire::CacheablePtr nativeptr(rs);
            //TResult grs =  Serializable::GetManagedValueGeneric<TResult>( nativeptr);
            m_rscoll->AddResult(safe_cast<TResult>(rs));
          }

          virtual void EndResults() 
          {
            m_rscoll->EndResults();
          }

          virtual void ClearResults() 
          {
            m_rscoll->ClearResults();
          }

      };
    }
    }
  }
}
