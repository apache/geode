/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: ReplicationData.java,v 1.3 2004/07/05 05:41:45 belaban Exp $

package com.gemstone.org.jgroups.blocks;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;



/**
 * Class used for data exchange by ReplicationManager and ReplicationReceiver.
 * @author Bela Ban
 */
public class ReplicationData implements Externalizable {
    public static final int SEND     = 1;
    public static final int COMMIT   = 2;
    public static final int ROLLBACK = 3;

    
    int      type=0;
    byte[]   data=null;
    Xid      transaction=null;
    byte[]   lock_info=null;
    long     lock_acquisition_timeout=0;
    long     lock_lease_timeout=0;
    boolean  use_locks=false;
        

    public ReplicationData() {
        ; // used by externalization
    }

    
    public ReplicationData(int      type,
                           byte[]   data,
                           Xid      transaction,
                           byte[]   lock_info,
                           long     lock_acquisition_timeout,
                           long     lock_lease_timeout,
                           boolean  use_locks) {
        this.type=type;
        this.data=data;
        this.transaction=transaction;
        this.lock_info=lock_info;
        this.lock_acquisition_timeout=lock_acquisition_timeout;
        this.lock_lease_timeout=lock_lease_timeout;
        this.use_locks=use_locks;
    }

    
    public int     getType()                   {return type;}
    public byte[]  getData()                   {return data;}
    public Xid     getTransaction()            {return transaction;}
    public byte[]  getLockInfo()               {return lock_info;}
    public long    getLockAcquisitionTimeout() {return lock_acquisition_timeout;}
    public long    getLockLeaseTimeout()       {return lock_lease_timeout;}
    public boolean useLocks()                  {return use_locks;}


    @Override // GemStoneAddition
    public String toString() {
        StringBuffer sb=new StringBuffer();
        sb.append(typeToString(type)).append(" [").append(", transaction=").append(transaction);
        switch(type) {
        case SEND:
            if(data != null)
                sb.append(", data=").append(data.length).append(" bytes");
            sb.append(", lock_acquisition_timeout=").append(lock_acquisition_timeout);
            sb.append(", lock_lease_timeout=").append(lock_lease_timeout);
            sb.append(", use_locks=").append(use_locks);
            break;
        case COMMIT:
        case ROLLBACK:
            break;
        }
        sb.append(']');
        return sb.toString();
    }


    public static String typeToString(int t) {
        switch(t) {
        case SEND:     return "SEND";
        case COMMIT:   return "COMMIT";
        case ROLLBACK: return "ROLLBACK";
        default:       return "<unknown>";
        }
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(type);
        if(data != null) {
            out.writeInt(data.length);
            out.write(data, 0, data.length);
        }
        else
            out.writeInt(0);
        if(transaction != null) {
            out.writeBoolean(true);
            transaction.writeExternal(out);
        }
        else
            out.writeBoolean(false);
        if(use_locks) {
            out.writeBoolean(true);
            if(lock_info != null) {
                out.writeInt(lock_info.length);
                out.write(lock_info, 0, lock_info.length);
            }
            else
                out.writeInt(0);
            out.writeLong(lock_acquisition_timeout);
            out.writeLong(lock_lease_timeout);
        }
        else
            out.writeBoolean(false);
    }
    
    
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int num;
        type=in.readInt();
        if((num=in.readInt()) > 0) {
            data=new byte[num];
            in.readFully(data, 0, num);
        }
        if(in.readBoolean()) {
            transaction=new Xid();
            transaction.readExternal(in);
        }
        use_locks=in.readBoolean();
        if(use_locks) {
            if((num=in.readInt()) > 0) {
                lock_info=new byte[num];
                in.readFully(lock_info, 0, num);
            }
            lock_acquisition_timeout=in.readLong();
            lock_lease_timeout=in.readLong();        
        }
    }
}
