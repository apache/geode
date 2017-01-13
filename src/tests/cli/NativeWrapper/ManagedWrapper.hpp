/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "NativeType.hpp"
#include "impl/NativeWrapper.hpp"
#include <vcclr.h>

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Internal
      {

        /// <summary>
        /// Internal class used to keep a reference of the managed object
        /// alive while a method on the native object is in progress
        /// (one possible fix for bug #309)
        /// </summary>
        template <typename TNative, typename TManaged>
        class NativePtrWrap
        {
        public:
          inline NativePtrWrap(TNative* nativePtr, TManaged^ mgObj) :
              m_nativePtr(nativePtr), m_mgObj(mgObj) { }
          inline TNative* operator->()
          {
            return m_nativePtr;
          }
          inline TNative* operator()()
          {
            return m_nativePtr;
          }

        private:
          TNative* m_nativePtr;
          gcroot<TManaged^> m_mgObj;
        };
      }

      namespace Tests
      {
        /// <summary>
        /// This class tests GC for managed wrapped objects when
        /// a method on the native object is still in progress.
        /// </summary>
        public ref class ManagedWrapper
        {
        public:
          /// <summary>
          /// constructor with given length of (dummy) string
          /// </summary>
          ManagedWrapper(int len);
          /// <summary>
          /// destructor (.NET's IDisposable.Dispose method)
          /// </summary>
          ~ManagedWrapper();
          /// <summary>
          /// finalizer
          /// </summary>
          !ManagedWrapper();

          /// <summary>
          /// Invokes NativeType::doOp method in an unsafe manner i.e.
          /// without making sure that the managed wrapper can be GCed
          /// while the NativeType::doOp is in progress.
          /// </summary>
          bool UnsafeDoOp(int size, int numOps);

          /// <summary>
          /// Invokes NativeType::doOp method in an safe manner i.e.
          /// making sure that the managed wrapper will not be GCed
          /// while the NativeType::doOp is in progress.
          /// This implementation uses native temporary wrapper.
          /// </summary>
          bool SafeDoOp1(int size, int numOps);

          /// <summary>
          /// Invokes NativeType::doOp method in an safe manner i.e.
          /// making sure that the managed wrapper will not be GCed
          /// while the NativeType::doOp is in progress.
          /// This implementation uses managed value type temporary wrapper.
          /// </summary>
          bool SafeDoOp2(int size, int numOps);

          /// <summary>
          /// Invokes NativeType::doOp method in an safe manner i.e.
          /// making sure that the managed wrapper will not be GCed
          /// while the NativeType::doOp is in progress.
          /// This implementation uses GC::KeepAlive.
          /// </summary>
          bool SafeDoOp3(int size, int numOps);

        private:
          /// <summary>
          /// Get the native pointer in an unsafe manner i.e. such that the
          /// managed object can be GCed while a method on the native object
          /// is still in progress.
          /// </summary>
          property NativeType* UnsafeNativePtr
          {
            inline NativeType* get()
            {
              return m_nativePtr;
            }
          }

          /// <summary>
          /// Get the native pointer in a safe manner i.e. such that the
          /// managed object cannot be GCed while a method on the native
          /// object is in progress since the temporary <c>NativePtrWrap</c>
          /// object holds a reference to the managed object.
          /// </summary>
          property Internal::NativePtrWrap<NativeType, ManagedWrapper>
            SafeNativePtr1
          {
            inline Internal::NativePtrWrap<NativeType, ManagedWrapper> get()
            {
              return Internal::NativePtrWrap<NativeType, ManagedWrapper>(
                m_nativePtr, this);
            }
          }

          /// <summary>
          /// Get the native pointer in a safe manner i.e. such that the
          /// managed object cannot be GCed while a method on the native
          /// object is in progress since the temporary <c>ManagedPtrWrap</c>
          /// object holds a reference to the managed object.
          /// </summary>
          property GemStone::GemFire::Cache::Generic::Internal::ManagedPtrWrap<NativeType, ManagedWrapper>
            SafeNativePtr2
          {
            inline GemStone::GemFire::Cache::Generic::Internal::ManagedPtrWrap<NativeType, ManagedWrapper> get()
            {
              return GemStone::GemFire::Cache::Generic::Internal::ManagedPtrWrap<NativeType, ManagedWrapper>(
                m_nativePtr, this);
            }
          }

          /// <summary>
          /// pointer to native object
          /// </summary>
          NativeType* m_nativePtr;
          /// <summary>
          /// Dummy string to make GC believe that this managed object
          /// has a large size and is worth GCing.
          /// </summary>
          System::String^ m_str;

          /// <summary>
          /// Internal cleanup method invoked by both the destructor
          /// and finalizer.
          /// </summary>
          void InternalCleanup();
        };

      }
    }
  }
}

