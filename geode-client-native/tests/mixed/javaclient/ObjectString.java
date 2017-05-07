/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package javaclient;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Vector;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.Instantiator;

/**
 * @author xzhou
 *
 */
public class ObjectString implements DataSerializable {
    private String content;
    private int id;

    static {
		Instantiator.register(new Instantiator(ObjectString.class, (byte) 52) {
			public DataSerializable newInstance() {
				return new ObjectString();
			}
		});
	}

    public String getContent( ) {
        return this.content;
    }

    public int getId( ) {
        return this.id;
    }

    public void setContent( String string_field ) {
        this.content = string_field;
    }

    public void setId( int id ) {
        this.id = id;
    }

	public void toData(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeUTF(content);
	}

	public void fromData(DataInput in) throws IOException, ClassNotFoundException {
		this.id = in.readInt();
		this.content = in.readUTF();
	}
}
