/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 *@file BufferManager.hpp
 *@since   1.0
 *@version 1.0
 */

#if !defined (IMPL_BUFFER_MANAGER_INCLUDED)
#define IMPL_BUFFER_MANAGER_INCLUDED

#include "../gfcpp_globals.hpp"
#include <deque>
#include <ace/Guard_T.h>
#include <ace/Thread_Mutex.h>

namespace gemfire
{
        class CPPCACHE_EXPORT StreamBuffer {
	  public:
	    StreamBuffer(uint32_t size=64*1024):
	      m_byte(NULL),
	      m_size(size)
	    {
	      m_byte = new char[size];
	    }
	    ~StreamBuffer()
	    {
	      if(m_byte!=NULL)
	      {
		delete [] m_byte;
		m_byte=NULL;
	      }
	    }
	    inline uint32_t getSize() const
	    {
	      return m_size;
	    }
	    inline char* getBuffer() 
	    {
	      return m_byte;
	    }
	  private:
	    char* m_byte;
	    uint32_t m_size;
	};

	class CPPCACHE_EXPORT BufferManager {
	public:
		BufferManager(unsigned long numElements, unsigned long size=64*1024):
		  m_maxElements(numElements),
		  m_bufferSize(size)
		{
			for (unsigned long n = 0; n < numElements; n++)
			{
				StreamBuffer* mp = new StreamBuffer(size);
				m_freelist.push_front(mp);
			}
		}
		~BufferManager()
		{
			ACE_Guard< ACE_Thread_Mutex > _guard( m_mutex );
			while (m_freelist.size() > 0)
			{
				StreamBuffer* mp = m_freelist.back();
				m_freelist.pop_back();
				delete mp;
			}
		}
		StreamBuffer* get(uint32_t size)
		{
		  if(size > m_bufferSize)
		    return new StreamBuffer(size);
		  StreamBuffer * mp = 0;
		  {
		    ACE_Guard< ACE_Thread_Mutex > _guard( m_mutex );
		    if (m_freelist.size() > 0)
		    {
			mp = m_freelist.back();
			m_freelist.pop_back();
		    }
		  }
		  return mp;
		}
		void release(StreamBuffer* mp)
		{
                   if(mp->getSize() > m_bufferSize)
		     delete mp;
		   else
		   {
		      ACE_Guard< ACE_Thread_Mutex > _guard( m_mutex );
		      m_freelist.push_front(mp);
		   }
		}
	private:
		std::deque<StreamBuffer *> m_freelist;
                ACE_Thread_Mutex m_mutex;
		uint32_t   m_maxElements;
		uint32_t   m_bufferSize;
	};
} // end namespace
#endif // !defined (IMPL_BUFFER_MANAGER_INCLUDED)
