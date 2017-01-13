/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once



#include "../gf_defs.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache 
		{ 
		namespace Generic
    {
      namespace Internal
      {

        /// <summary>
        /// Internal class used to keep a reference of the managed object
        /// alive while a method on the native object is in progress
        /// (fix for bug #309)
        /// </summary>
        template <typename TNative, typename TManaged>
        public value class ManagedPtrWrap
        {
        public:
          inline ManagedPtrWrap(TNative* nativePtr, TManaged^ mgObj) :
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
          TManaged^ m_mgObj;
        };

        /// <summary>
        /// Internal class to wrap native object pointers that derive from
        /// <c>SharedBase</c>; <c>NTYPE</c> is the native class.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This is a convenience template class to wrap native class objects
        /// that derive from <c>SharedBase</c>.
        /// The native class objects can be of two kinds -- one that derive from
        /// SharedBase and are handled using smart pointers, and the second that
        /// do not derive from SharedBase. For the first category we have to invoke
        /// <c>preserveSB()</c> and <c>releaseSB()</c> on the native object during
        /// construction and dispose/finalization of the wrapped object.
        /// </para><para>
        /// All the managed wrapper classes that wrap corresponding native classes
        /// deriving from <c>SharedBase</c> should derive from this class.
        /// </para><para>
        /// <b>IMPORTANT:</b> Direct assignment of m_nativeptr has been disabled,
        /// rather the classes deriving from this should use
        /// <c>SBWrap.SetPtr</c> or <c>SBWrap._SetNativePtr</c>
        /// </para><para>
        /// This class is intentionally <b>not</b> thread-safe.
        /// </para>
        /// </remarks>
        template<typename NTYPE>
        public ref class SBWrap
        {
        public:

          /// <summary>
          /// The dispose function.
          /// </summary>
          inline ~SBWrap( )
          {
            InternalCleanup( );
          }

          /// <summary>
          /// The finalizer, for the case dispose is not called.
          /// </summary>
          inline !SBWrap( )
          {
            InternalCleanup( );
          }


        internal:

          /// <summary>
          /// Default constructor initializes the native pointer to null.
          /// </summary>
          inline SBWrap( ): m_nativeptr( nullptr ) { }

          /// <summary>
          /// Constructor to wrap the given native pointer.
          /// </summary>
          inline SBWrap( NTYPE* nativeptr ): m_nativeptr( nativeptr )
          {
            if (nativeptr != nullptr)
            {
              nativeptr->preserveSB( );
            }
          }

          /// <summary>
          /// Get the reference to underlying native object.
          /// </summary>
          /// <returns>
          /// The native pointer wrapped inside ManagedPtrWrap object.
          /// </returns>
          property ManagedPtrWrap< NTYPE, SBWrap<NTYPE> > NativePtr
          {
            inline ManagedPtrWrap< NTYPE, SBWrap<NTYPE> > get()
            {
              return ManagedPtrWrap< NTYPE, SBWrap<NTYPE> >(m_nativeptr, this);
            }
          }

          /// <summary>
          /// Get the underlying native pointer.
          /// DO NOT USE UNLESS YOU KNOW WHAT YOU ARE DOING. SEE BUG #309.
          /// </summary>
          /// <returns>The native pointer.</returns>
          property NTYPE* _NativePtr
          {
            inline NTYPE* get()
            {
              return m_nativeptr;
            }
          }


        protected:

          /// <summary>
          /// Used to set the native pointer to a new object. This should only be
          /// used when you know that the underlying object is NULL.
          /// </summary>
          inline void SetPtr( NTYPE* nativeptr )
          {
            if (nativeptr != nullptr)
            {
              nativeptr->preserveSB( );
            }
            m_nativeptr = nativeptr;
          }

          /// <summary>
          /// Used to assign the native pointer to a new object.
          /// </summary>
          /// <remarks>
          /// Note the order of preserveSB() and releaseSB(). This handles the
          /// corner case when <c>m_nativeptr</c> is same as <c>nativeptr</c>.
          /// </remarks>
          inline void AssignPtr( NTYPE* nativeptr )
          {
            if (nativeptr != nullptr) {
              nativeptr->preserveSB();
            }
            if (m_nativeptr != nullptr) {
              m_nativeptr->releaseSB();
            }
            m_nativeptr = nativeptr;
          }

          /// <summary>
          /// Set the native pointer to the new object without doing a
          /// preserveSB(). DO NOT USE UNLESS YOU KNOW WHAT YOU ARE DOING.
          /// </summary>
          inline void _SetNativePtr( NTYPE* nativeptr )
          {
            m_nativeptr = nativeptr;
          }

          /// <summary>
          /// Internal cleanup function invoked by dispose/finalizer.
          /// </summary>
          inline void InternalCleanup( )
          {
            if (m_nativeptr != nullptr)
            {
              m_nativeptr->releaseSB( );
              m_nativeptr = nullptr;
            }
          }


        private:

          NTYPE* m_nativeptr;
        };


        /// <summary>
        /// Internal class to wrap native object pointers that do not derive from
        /// <c>SharedBase</c>; <c>NTYPE</c> is the native class.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This is a convenience template class to wrap native class objects
        /// that do not derive from SharedBase.
        /// </para><para>
        /// The constructors (and <c>SetPtr</c> method) of this class require a
        /// boolean parameter that informs whether or not to take ownership
        /// of the native object. When the ownership is taken, then the
        /// native object is destroyed when the wrapper object is
        /// disposed/finalized, otherwise it is not.
        /// </para><para>
        /// All the managed wrapper classes that wrap corresponding native classes
        /// not deriving from <c>SharedBase</c> should derive from this class.
        /// </para><para>
        /// <b>IMPORTANT</b>: Direct assignment of m_nativeptr has been disabled,
        /// rather the classes deriving from this should use <c>UMWrap.SetPtr</c>
        /// that also requires a boolean parameter to indicate whether or not
        /// to take ownership of the native object.
        /// </para><para>
        /// This class is intentionally <b>not</b> thread-safe.
        /// </para>
        /// </remarks>
        template<typename NTYPE>
        public ref class UMWrap
        {
        public:

          /// <summary>
          /// The dispose function.
          /// </summary>
          inline ~UMWrap( )
          {
            InternalCleanup( );
          }

          /// <summary>
          /// The finalizer, for the case dispose is not called.
          /// </summary>
          inline !UMWrap( )
          {
            InternalCleanup( );
          }


        internal:

          /// <summary>
          /// Default constructor assigns null to the native pointer and takes
          /// ownership of the object.
          /// </summary>
          inline UMWrap( )
            : m_nativeptr( nullptr ), m_own( true ) { }

          /// <summary>
          /// Constructor to wrap the given native object and inform about
          /// the ownership of the native object.
          /// </summary>
          inline UMWrap( NTYPE* nativeptr, bool own )
            : m_nativeptr( nativeptr ), m_own( own )
          {
            if (!own)
            {
              GC::SuppressFinalize( this );
            }
          }

          /// <summary>
          /// Get the reference to underlying native object.
          /// </summary>
          /// <returns>
          /// The native pointer wrapped inside NativePtrWrap object.
          /// </returns>
          property ManagedPtrWrap< NTYPE, UMWrap<NTYPE> > NativePtr
          {
            inline ManagedPtrWrap< NTYPE, UMWrap<NTYPE> > get()
            {
              return ManagedPtrWrap< NTYPE, UMWrap<NTYPE> >(m_nativeptr, this);
            }
          }

          /// <summary>
          /// Get the underlying native pointer.
          /// DO NOT USE UNLESS YOU KNOW WHAT YOU ARE DOING. SEE BUG #309.
          /// </summary>
          /// <returns>The native pointer.</returns>
          property NTYPE* _NativePtr
          {
            inline NTYPE* get()
            {
              return m_nativeptr;
            }
          }


        protected:

          /// <summary>
          /// Get or set the ownership of this object.
          /// </summary>
          /// <returns>True if the native object is owned by this object.</returns>
          property bool Own
          {
            inline bool get( )
            {
              return m_own;
            }
            inline void set( bool own )
            {
              if (m_own != own)
              {
                if (own)
                {
                  GC::ReRegisterForFinalize( this );
                }
                else
                {
                  GC::SuppressFinalize( this );
                }
              }
              m_own = own;
            }
          }

          /// <summary>
          /// Used to set the native pointer to a new object. This should only
          /// be used when you know that the underlying object is NULL
          /// or you do not own it.
          /// </summary>
          inline void SetPtr( NTYPE* nativeptr, bool own )
          {
            m_nativeptr = nativeptr;
            m_own = own;
          }

          /// <summary>
          /// Internal cleanup function invoked by dispose/finalizer.
          /// </summary>
          inline void InternalCleanup( )
          {
            if (m_own && m_nativeptr != nullptr)
            {
              delete m_nativeptr;
              m_nativeptr = nullptr;
            }
          }


        private:

          NTYPE* m_nativeptr;
          bool m_own;
        };

        /// <summary>
        /// Internal class to wrap native object pointers that do not derive from
        /// <c>SharedBase</c>; <c>NTYPE</c> is the native class.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This is a convenience template class to wrap native class objects
        /// that do not derive from SharedBase.
        /// </para><para>
        /// The constructors (and <c>SetPtr</c> method) of this class require a
        /// boolean parameter that informs whether or not to take ownership
        /// of the native object. Even if the ownership is taken, the
        /// native object is purposefully not destroyed as the class that impelements 
        /// it has the protected or the private destructor.
        /// </para><para>
        /// All the managed wrapper classes that wrap corresponding native classes
        /// not deriving from <c>SharedBase</c> should derive from this class.
        /// </para><para>
        /// <b>IMPORTANT</b>: Direct assignment of m_nativeptr has been disabled,
        /// rather the classes deriving from this should use <c>UMWrap.SetPtr</c>
        /// that also requires a boolean parameter to indicate whether or not
        /// to take ownership of the native object.
        /// </para><para>
        /// This class is intentionally <b>not</b> thread-safe.
        /// </para>
        /// </remarks>
        template<typename NTYPE>
        public ref class UMWrapN
        {
        public:

          /// <summary>
          /// The dispose function.
          /// </summary>
          inline ~UMWrapN( )
          {
            InternalCleanup( );
          }

          /// <summary>
          /// The finalizer, for the case dispose is not called.
          /// </summary>
          inline !UMWrapN( )
          {
            InternalCleanup( );
          }

        internal:

          /// <summary>
          /// Default constructor assigns null to the native pointer and takes
          /// ownership of the object.
          /// </summary>
          inline UMWrapN( )
            : m_nativeptr( nullptr ), m_own( true ) { }

          /// <summary>
          /// Constructor to wrap the given native object and inform about
          /// the ownership of the native object.
          /// </summary>
          inline UMWrapN( NTYPE* nativeptr, bool own )
            : m_nativeptr( nativeptr ), m_own( own )
          {
            if (!own)
            {
              GC::SuppressFinalize( this );
            }
          }

          /// <summary>
          /// Get the reference to underlying native object.
          /// </summary>
          /// <returns>
          /// The native pointer wrapped inside NativePtrWrap object.
          /// </returns>
          property ManagedPtrWrap< NTYPE, UMWrapN<NTYPE> > NativePtr
          {
            inline ManagedPtrWrap< NTYPE, UMWrapN<NTYPE> > get()
            {
              return ManagedPtrWrap< NTYPE, UMWrapN<NTYPE> >(m_nativeptr, this);
            }
          }

          /// <summary>
          /// Get the underlying native pointer.
          /// DO NOT USE UNLESS YOU KNOW WHAT YOU ARE DOING. SEE BUG #309.
          /// </summary>
          /// <returns>The native pointer.</returns>
          property NTYPE* _NativePtr
          {
            inline NTYPE* get()
            {
              return m_nativeptr;
            }
          }

        protected:

          /// <summary>
          /// Get or set the ownership of this object.
          /// </summary>
          /// <returns>True if the native object is owned by this object.</returns>
          property bool Own
          {
            inline bool get( )
            {
              return m_own;
            }
            inline void set( bool own )
            {
              if (m_own != own)
              {
                if (own)
                {
                  GC::ReRegisterForFinalize( this );
                }
                else
                {
                  GC::SuppressFinalize( this );
                }
              }
              m_own = own;
            }
          }

          /// <summary>
          /// Used to set the native pointer to a new object. This should only
          /// be used when you know that the underlying object is NULL
          /// or you do not own it.
          /// </summary>
          inline void SetPtr( NTYPE* nativeptr, bool own )
          {
            m_nativeptr = nativeptr;
            m_own = own;
          }

          /// <summary>
          /// Internal cleanup function invoked by dispose/finalizer.
          /// </summary>
          inline void InternalCleanup( )
          {
            if (m_own && m_nativeptr != nullptr)
            {
             // No deletion of m_nativeptr as the destructor is protected or private.
            }
          }

        private:

          NTYPE* m_nativeptr;
          bool m_own;
        };
      }
    }
  }
}
}

