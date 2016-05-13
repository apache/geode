#ifndef __GEMFIRE_IMPL_CONTAINERWITHLOCK_H__
#define __GEMFIRE_IMPL_CONTAINERWITHLOCK_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *
 * The specification of function behaviors is found in the corresponding .cpp file.
 *
 *========================================================================
 */

#include <StlIncludes.hpp>
#include "../CacheableString.hpp"

// This class requires a reentrant lock model..
#include <ace/Recursive_Thread_Mutex.h>
#include <ace/Guard_T.h>

/**
 * @file
 * vector of region base class 
 *
 */

namespace gemfire {

#define CWLGuard ACE_Guard< ACE_Recursive_Thread_Mutex > guard( m_mapMutex )

/** @todo  What is this */
template<class Itor, class Ptr>
class CPPCACHE_EXPORT ContainerWithLock {
public:

    typedef Itor iterator;

    ContainerWithLock()
    : m_map(),
      m_mapMutex()
    {
    }

    virtual ~ContainerWithLock()
    {
       CWLGuard;
       m_map.clear();
    }

    inline void clear()
    {
        CWLGuard;
        m_map.clear();
    }
    inline bool empty()
    {
        bool s=false;
        CWLGuard;
        s = m_map.empty();
        return s;
    }
    inline size_t size()
    {
        size_t s=0;
        CWLGuard;
        s = m_map.size();
        return s;
    }
    inline void insert(const CacheableStringPtr& key , Ptr& value )
    {
        CWLGuard;
        m_map.insert(std::make_pair(key, value));
    }
    inline void insert(const std::pair<CacheableStringPtr, Ptr>& thePair)
    {
        CWLGuard;
        m_map.insert(thePair);
    }
    inline void lock()
    {
        m_mapMutex.acquire();
    }
    inline void unlock()
    {
        m_mapMutex.release();
    }
    inline iterator begin()
    {
        return m_map.begin();
    }
    inline iterator end()
    {
        return m_map.end();
    }
    inline int erase(const CacheableStringPtr& key)
    {
        int count=0;
        CWLGuard;
        count = m_map.erase(key);
        return count;
    }
    inline void erase(iterator pos)
    {
        CWLGuard;
        m_map.erase(pos);
    }
    inline bool find(const CacheableStringPtr& key, Ptr& rptr)
    {
        rptr = NULL;
        CWLGuard;
        if(m_map.empty()==true)
        {
           return false;
        }
        iterator q=m_map.find(key);
        if(q!=m_map.end())
        {
           rptr = q->second;
           return true;
        }
        return false;
    }
    inline iterator find(const CacheableStringPtr& key)
    {
        CWLGuard;
        if(m_map.empty()==true)
        {
           return m_map.end();
        }
        iterator q=m_map.find(key);
        return q;
     }

private:
    std::map<CacheableStringPtr, Ptr, CacheableStringPtr_cmp> m_map;
    ACE_Recursive_Thread_Mutex m_mapMutex;
};

}
#endif //define __GEMFIRE_IMPL_CONTAINERWITHLOCK_H__
