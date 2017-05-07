/*========================================================================
 *
 * Name - InterlockedCompareExchange -- 
 *
 * Purpose -
 * The function returns the initial value of the Destination parameter.
 * The function compares the Destination value with the Comparand value. 
 * If the Destination value is equal to the Comparand value, the Exchange 
 * value is stored in the address specified by Destination. 
 * Otherwise, no operation is performed.
 *
 * membar not used
 *
 *========================================================================
 */
      .seg    "text"
      .proc   12
      .global InterlockedCompareExchange
      .type InterlockedCompareExchange, #function
InterlockedCompareExchange:

       cas     [%o0],%o2,%o1   ! /* atomic CAS, with read value -> retval */
       retl
       mov     %o1,%o0         ! /* retval = 1 IFF tmp == 0 */

/*========================================================================
 *
 * Name - InterlockedExchangeAdd -- Gemfire name was HostAsmAtomicAdd
 *
 * Purpose -
 * Add 'increment' to  the counter pointed to be 'ctrPtr'.
 * Returns the old value.
 *
 * membar not used
 *
 *========================================================================
 */
      .proc   12
      .global InterlockedExchangeAdd 
      .type InterlockedExchangeAdd, #function
InterlockedExchangeAdd: 
                                   ! %o0 = ctrPtr 
                                   ! %o1 = increment
retryAdd:                          ! do {
        ld      [%o0],%o2          ! %o2 = *ctrPtr
        add     %o2,%o1,%o3        ! %o3 = %o2 + increment
        cas     [%o0],%o2,%o3      ! if (%o2 == *ctrPtr)
                                   !   tmp = *ctrPtr, *ctrPtr = %o3, %o3 = tmp
                                   ! else
                                   !   %o3 = *ctrPtr
        cmp     %o2,%o3            !
        bne     retryAdd           ! } while (%o2 != %o3)
        nop                        ! fix for bug 22851
        retl 
        mov     %o3,%o0            ! return old value of *ctrPtr in %o0

/*========================================================================
 *
 * Name - InterlockedExchangeAddLong -- Gemfire name was HostAsmAtomicAddLong
 *
 * Purpose -
 * Handels 64 bit Pointer for v9 architecture
 * Add 'increment' to  the counter pointed to be 'ctrPtr'.
 * Returns the old value.
 *
 * membar not used
 *
 *========================================================================
 */
      .proc   12
      .global InterlockedExchangeAddLong
      .type InterlockedExchangeAddLong, #function
InterlockedExchangeAddLong: 
                                   ! %o0 = ctrPtr 
                                   ! %o1 = increment
retryAddLong:                      ! do {
        ldx     [%o0],%o2          ! %o2 = *ctrPtr
        add     %o2,%o1,%o3        ! %o3 = %o2 + increment
        casx    [%o0],%o2,%o3      ! if (%o2 == *ctrPtr)
                                   !   tmp = *ctrPtr, *ctrPtr = %o3, %o3 = tmp
                                   ! else
                                   !   %o3 = *ctrPtr
        cmp     %o2,%o3            !
        bne     retryAddLong       ! } while (%o2 != %o3)
        nop                        ! fix for bug 22851
        retl 
        mov     %o3,%o0            ! return old value of *ctrPtr in %o0

/*========================================================================
 *
 * Name - HostAsmUnlock
 *
 * Purpose -
 *      Unlock the specified lock.
 *
 *========================================================================
 */
        .proc   16
        .global HostAsmUnlock
        .type HostAsmUnlock, #function
HostAsmUnlock:
        membar  #StoreStore | #LoadStore | #StoreLoad |  #LoadLoad
        retl
        st      %o0,[%o1]  ! store word, 32 bit
!        st      %g0,[%o0]  ! store word, 32 bit

/*========================================================================
 *
 * Name - HostAsmTryLock
 *
 * bool HostAsmTryLock(SpinLockField *lockPtr, int32 count, uint32 lockVal);
 * Purpose -
 *      Try to get access to the specified lock.  If it succeeds in getting
 *      the lock in the number of tries specified in by count,
 *      TRUE is returned.  If the lock is not available with the count
 *      tries, it returns FALSE.
 *
 *========================================================================
 */
        .seg    "text"
        .proc   16
        .global HostAsmTryLock
        .type HostAsmTryLock, #function
HostAsmTryLock:
                                        ! %o0 = lockPtr
                                        ! %o1 = count
                                        ! %o2 = lock value to store

        ld      [%o0],%o3               ! load *lockPtr
        tst     %o3                     ! test if 0
        bne     spinLoop                ! if not 0 we must spin
        mov     %o2, %o3                ! branch delay slot, new value into o3
tryLock:
                                   ! %o0 = memPtr
                                   ! %g0 = oldValue (zero)
                                   ! %o3 = newValue
                                   ! if (%g0 == *memPtr)
                                   !   tmp = *memPtr, *memPtr = %o3, %o3 = tmp
                                   ! else
                                   !   %o3 = *memPtr
        cas     [%o0], %g0, %o3

        tst     %o3                     ! what was value in lock word
        be,a    return                  ! if 0 go to return
        mov     1,%o3                   ! set return value



spinLoop:
        ld      [%o0],%o3               ! load *lockPtr
        tst     %o3                     ! test if 0
        be      tryLock                 ! if 0 we can retry the atomic swap
        mov     %o2, %o3                ! branch delay slot, new value into o3

        nop                             !  delay to limit frequency of probing
        nop                             !    shared memory
        nop
        nop
        nop

        dec     %o1                     ! count--
        tst     %o1                     ! test count
        bg      spinLoop                ! count > 0 go to spinLoop
        nop                             ! branch delay slot

        mov     0,%o3                   ! fail because count ran out

return:
        membar  #LoadLoad | #LoadStore |  #StoreStore  | #StoreLoad
        retl
        add     %g0,%o3,%o0             ! move result to %o0 for return
