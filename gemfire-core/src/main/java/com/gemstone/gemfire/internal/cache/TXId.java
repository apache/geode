
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.internal.cache;
import com.gemstone.gemfire.internal.ExternalizableDSFID;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.cache.TransactionId;
import com.gemstone.gemfire.internal.DSFIDFactory;
import java.io.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

/** The implementation of the {@link TransactionId} interface stored
 * in the transaction state and used, amoung other things, to uniquely
 * identify a transaction in a confederation of transaction
 * participants (currently VM in a Distributed System).
 *
 * @author Mitch Thomas
 * 
 * @since 4.0
 * 
 * @see TXManagerImpl#begin
 * @see com.gemstone.gemfire.cache.CacheTransactionManager#getTransactionId
 */
public final class TXId
  extends ExternalizableDSFID
  implements TransactionId {
  /** The domain of a transaction, currently the VM's unique identifier */
  private InternalDistributedMember memberId;
  /** Per unique identifier within the transactions memberId */
  private int uniqId;

  /** Default constructor meant for the Externalizable
   */
  public TXId() {
  }
  
  /** Constructor for the Transation Manager, the birth place of
   * TXId objects.  The object is Serializable mainly because of
   * the identifier type provided by JGroups.
   */
  public TXId(InternalDistributedMember memberId, int uniqId)  {
    this.memberId = memberId;
    this.uniqId = uniqId;
  }

  public InternalDistributedMember getMemberId() {
    return this.memberId;
  }

  @Override
  public String toString() {
    return ("TXId: " + this.memberId + ':' + this.uniqId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof TXId)) {
      return false;
    }

    TXId otx = (TXId) o;
    return (otx.uniqId == this.uniqId &&
        ((otx.memberId==null && this.memberId==null) || 
         (otx.memberId!=null && this.memberId!=null && otx.memberId.equals(this.memberId))));
            
  }

  @Override
  public int hashCode() {
    int retval=this.uniqId;
    if (this.memberId != null)
      retval = retval * 37 + this.memberId.hashCode();
    return retval;
  }

  @Override
  public int getDSFID() {
    return TRANSACTION_ID;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.uniqId);
    InternalDataSerializer.invokeToData(this.memberId, out);
  }

  @Override
  public void fromData(DataInput in) 
    throws IOException, ClassNotFoundException {
    this.uniqId = in.readInt();
    this.memberId = DSFIDFactory.readInternalDistributedMember(in);
  }

  public static final TXId createFromData(DataInput in) 
    throws IOException, ClassNotFoundException
  {
    TXId result = new TXId();
    InternalDataSerializer.invokeFromData(result, in);
    return result;
  }
  
  public int getUniqId() {
    return this.uniqId;
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
  
}
