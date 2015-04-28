package com.gemstone.gemfire.internal.tools.gfsh.app.function;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.data.MemberInfo;

public class GfshData implements DataSerializable
{
	private static final long serialVersionUID = 1L;
	
	private MemberInfo memberInfo;
	private Object dataObject;
	private Object userData;
	
	public GfshData() {}
	
	public GfshData(Object dataObject)
	{
		Cache cache = CacheFactory.getAnyInstance();
		memberInfo = new MemberInfo();
		DistributedMember member = cache.getDistributedSystem().getDistributedMember();
		memberInfo.setHost(member.getHost());
		memberInfo.setMemberId(member.getId());
		memberInfo.setMemberName(cache.getName());
		memberInfo.setPid(member.getProcessId());
		
		this.dataObject = dataObject;
	}
	
	public GfshData(MemberInfo memberInfo, Object dataObject)
	{
		this.memberInfo = memberInfo;
		this.dataObject = dataObject;
	}

	public MemberInfo getMemberInfo()
	{
		return memberInfo;
	}

	public void setMemberInfo(MemberInfo memberInfo)
	{
		this.memberInfo = memberInfo;
	}

	public Object getDataObject()
	{
		return dataObject;
	}

	public void setDataObject(Object dataObject)
	{
		this.dataObject = dataObject;
	}
	
	public Object getUserData()
	{
		return userData;
	}

	public void setUserData(Object userData)
	{
		this.userData = userData;
	}

	public void fromData(DataInput in) throws IOException, ClassNotFoundException
	{
		memberInfo = (MemberInfo)DataSerializer.readObject(in);
		dataObject = DataSerializer.readObject(in);
		userData = DataSerializer.readObject(in);
	}

	public void toData(DataOutput out) throws IOException
	{
		DataSerializer.writeObject(memberInfo, out);
		DataSerializer.writeObject(dataObject, out);
		DataSerializer.writeObject(userData, out);
	}
}
