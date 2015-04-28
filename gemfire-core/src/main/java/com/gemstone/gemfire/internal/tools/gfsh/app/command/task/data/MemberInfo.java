package com.gemstone.gemfire.internal.tools.gfsh.app.command.task.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.internal.tools.gfsh.app.misc.util.DataSerializerEx;

/**
 * A data class that contains member information
 * @author dpark
 *
 */
public class MemberInfo implements DataSerializable
{
	private static final long serialVersionUID = 1L;
	
	private String memberId;
	private String memberName;
	private String host;
	private int pid;
	
	public String getMemberId()
	{
		return memberId;
	}

	public void setMemberId(String memberId)
	{
		this.memberId = memberId;
	}

	public String getMemberName()
	{
		return memberName;
	}

	public void setMemberName(String memberName)
	{
		this.memberName = memberName;
	}

	public String getHost()
	{
		return host;
	}

	public void setHost(String host)
	{
		this.host = host;
	}

	public int getPid()
	{
		return pid;
	}

	public void setPid(int pid)
	{
		this.pid = pid;
	}

	public void fromData(DataInput in) throws IOException, ClassNotFoundException
	{
		pid = in.readInt();
		memberId = DataSerializerEx.readUTF(in);
		memberName = DataSerializerEx.readUTF(in);
		host = DataSerializerEx.readUTF(in);
	}

	public void toData(DataOutput out) throws IOException
	{
		out.writeInt(pid);
		DataSerializerEx.writeUTF(memberId, out);
		DataSerializerEx.writeUTF(memberName, out);
		DataSerializerEx.writeUTF(host, out);
	}
}
