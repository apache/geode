/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#pragma once

//#include "../gf_includesN.hpp"
#include "../../../PropertiesM.hpp"
#include "../../../IAuthInitialize.hpp"
#include "../PropertiesMN.hpp"
#include "../IAuthInitializeN.hpp"
#include "SafeConvertN.hpp"

using namespace System;
//using namespace System::Collections::Generic;

using namespace GemStone::GemFire::Cache;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      //generic < class TPropKey, class TPropValue >
      ref class PropertiesToString
      {
      public:
        PropertiesToString(Properties<String^, String^>^ target)
        {
          m_target = target;
        }

        void Visit(GemStone::GemFire::Cache::ICacheableKey^ key,
          GemStone::GemFire::Cache::IGFSerializable^ val)
        {
          /*
          String^ skey = Serializable::GetManagedValueGeneric<String^>(key);
          String^ sval = Serializable::GetManagedValueGeneric<String^>(val);
          */
          /*
          String^ skey = dynamic_cast<String^>(key);
          String^ sval = dynamic_cast<String^>(val);
          */
          String^ skey = key->ToString();
          String^ sval = val->ToString();
          if (skey != nullptr && sval != nullptr)
          {
            m_target->Insert(skey, sval);
          }
        }

      private:

        Properties<String^, String^>^ m_target;
      };

      //generic < class TPropKey, class TPropValue >
      ref class PropertiesToNonGeneric
      {
      public:
        PropertiesToNonGeneric(GemStone::GemFire::Cache::Properties^ target)
        {
          m_target = target;
        }

        void Visit(String^ key, Object^ val)
        {
          /*
          CacheableStringPtr skey(Serializable::GetUnmanagedValueGeneric(key));
          CacheableStringPtr sval(Serializable::GetUnmanagedValueGeneric(val));
          m_target->Insert(skey, sval);
          */
          String^ skey = safe_cast<String^>(key);
          String^ sval = safe_cast<String^>(val);
          if (skey != nullptr && sval != nullptr)
          {
            m_target->Insert(skey, sval);
          }
        }

      private:

        GemStone::GemFire::Cache::Properties^ m_target;
      };

      //generic < class TPropKey, class TPropValue >
      public ref class AuthInitializeGeneric : GemStone::GemFire::Cache::IAuthInitialize
      {
        private:

          GemStone::GemFire::Cache::Generic::IAuthInitialize^ m_authinit;

        public:

          void SetAuthInitialize(GemStone::GemFire::Cache::Generic::IAuthInitialize^ authinit)
          {
            m_authinit = authinit;
          }

          virtual GemStone::GemFire::Cache::Properties^
            GetCredentials(GemStone::GemFire::Cache::Properties^ props, String^ server)
          {
            Properties<String^, String^>^ genericProps =
              Properties<String^, String^>::Create<String^, String^>();

            GemStone::GemFire::Cache::PropertyVisitor^ visitor =
              gcnew GemStone::GemFire::Cache::PropertyVisitor(
              gcnew PropertiesToString(genericProps), &PropertiesToString::Visit);

            props->ForEach(visitor);

            Properties<String^, Object^>^ genericCreds =
              m_authinit->GetCredentials/*<TPropKey, TPropValue>*/(genericProps, server);

            GemStone::GemFire::Cache::Properties^ credentials =
              GemStone::GemFire::Cache::Properties::Create();

            GemStone::GemFire::Cache::Generic::PropertyVisitorGeneric<String^, Object^>^ visitorReverse =
              gcnew GemStone::GemFire::Cache::Generic::PropertyVisitorGeneric<String^, Object^>(
              gcnew PropertiesToNonGeneric/*<String^, Object^>*/(credentials),
              &PropertiesToNonGeneric/*<String^, Object^>*/::Visit);

            genericCreds->ForEach(visitorReverse);
            
            return credentials;
          }

          virtual void Close()
          {
            m_authinit->Close();
          }
      };
    }
    }
  }
}
