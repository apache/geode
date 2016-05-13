/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _GEMFIRE_FAIRQUEUE_SOL2_HPP_
#define _GEMFIRE_FAIRQUEUE_SOL2_HPP_

#include "SpinLock.hpp"
#include <deque>
#include <ace/ACE.h>
#include <ace/Thread_Mutex.h>
#include <ace/Token.h>
#include <ace/Condition_T.h>
#include <ace/Time_Value.h>
#include <ace/Guard_T.h>


namespace gemfire
{

  template < class T > class FairQueue
  {
    public:

      FairQueue( ) : m_cond( m_queueLock ), m_closed( false )
      {
      }

      ~FairQueue( )
      {
        close( );
      }

      /** wait sec time until notified */
      T* getUntil( long sec )
      {
        bool isClosed;
        T* mp = getNoGetLock( isClosed );

        if ( mp == NULL && !isClosed ) {

          ACE_Guard < ACE_Token > _guard( m_queueGetLock );

          ACE_Time_Value stopAt( ACE_OS::gettimeofday( ) );
          stopAt += sec;

          ACE_Guard< ACE_Thread_Mutex > _guard1( m_queueLock );

          isClosed = getClosed( );
          if ( !isClosed ) {
            do {
              m_cond.wait( &stopAt );
              mp = getNoGetLock( isClosed );
            } while ( mp == NULL && !isClosed &&
                ( ACE_OS::gettimeofday( ) < stopAt ) );
          }
        }
        return mp;
      }

      void put( T* mp )
      {
        GF_DEV_ASSERT( mp != 0 );

        ACE_Guard< ACE_Thread_Mutex > _guard( m_queueLock );
        SpinLockGuard _guard1( m_queueSpinLock );
        if ( !m_closed ) {
          m_queue.push_front( mp );
          m_cond.signal( );
        }
      }

      uint32_t size( )
      {
        SpinLockGuard _guard( m_queueSpinLock );

        return static_cast<uint32_t> (m_queue.size( ));
      }

      bool getClosed( )
      {
        SpinLockGuard _guard( m_queueSpinLock );

        return m_closed;
      }

      bool empty( )
      {
        return ( size( ) == 0 );
      }

      void reset( )
      {
        SpinLockGuard _guard( m_queueSpinLock );

        m_closed = false;
      }

      void close( )
      {
        std::deque< T* > tmpQueue;
        {
          ACE_Guard< ACE_Thread_Mutex > _guard( m_queueLock );
          SpinLockGuard _guard1( m_queueSpinLock );

          m_closed = true;
          while ( !m_queue.empty() ) {
            T* mp = m_queue.back( );
            m_queue.pop_back( );
            tmpQueue.push_front( mp );
          }
          m_cond.signal( );
        }
        while ( tmpQueue.size( ) > 0 ) {
          T* mp = tmpQueue.back( );
          tmpQueue.pop_back( );
          delete mp;
        }
      }


    private:

      inline T* getNoGetLock( bool& isClosed )
      {
        T* mp = NULL;
        SpinLockGuard _guard( m_queueSpinLock );

        isClosed = m_closed;
        if ( !isClosed && m_queue.size( ) > 0 ) {
          mp = m_queue.back( );
          m_queue.pop_back( );
        }
        return mp;
      }

      std::deque < T* > m_queue;
      SpinLock m_queueSpinLock;
      ACE_Thread_Mutex m_queueLock;
      ACE_Condition < ACE_Thread_Mutex > m_cond;
      ACE_Token m_queueGetLock;
      bool m_closed;
  };

} // end namespace


#endif // ifndef _GEMFIRE_FAIRQUEUE_SOL2_HPP_
