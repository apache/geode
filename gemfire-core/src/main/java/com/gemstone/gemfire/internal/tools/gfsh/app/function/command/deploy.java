package com.gemstone.gemfire.internal.tools.gfsh.app.function.command;

import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.ServerExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.function.GfshData;
import com.gemstone.gemfire.internal.tools.gfsh.app.misc.util.ClassFinder;
import com.gemstone.gemfire.internal.tools.gfsh.app.misc.util.SystemClassPathManager;
import com.gemstone.gemfire.internal.tools.gfsh.app.pogo.KeyType;
import com.gemstone.gemfire.internal.tools.gfsh.app.pogo.KeyTypeManager;

public class deploy implements ServerExecutable
{
	private final static String ENV_GEMFIRE_HOME = "GEMFIRE";
	
	private byte code = AggregateResults.CODE_NORMAL;
	private String codeMessage = null;
	private final static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyyMMddHHmm");
	
	public Object execute(String command, String regionPath, Object arg) throws Exception
	{
		GfshData data = new GfshData(null);
		
		Cache cache = CacheFactory.getAnyInstance();
		
		Object[] args = (Object[])arg;
		
		String operationType = (String)args[0];
		if (operationType.equals("-jar")) {
			String jarNames[] = (String[])args[1];
			byte byteBuffers[][] = (byte[][])args[2];
			try {
				String home = System.getenv(ENV_GEMFIRE_HOME);
				String classDir = home + "/gfsh/plugins";
				File classDirFile = new File(classDir);
				classDirFile.mkdirs();
		
				// Store the jar files
				String datedFilePaths[] = new String[jarNames.length];
				for (int i = 0; i < byteBuffers.length; i++) {
					String filePath = classDir + "/" + getDatedJarName(jarNames[i]);
					datedFilePaths[i] = filePath;
					File file = new File(filePath);
					FileOutputStream fos = new FileOutputStream(file);
					fos.write(byteBuffers[i]);
					fos.close();
				}
				
				// Add the jars to the class path
				for (int i = 0; i < datedFilePaths.length; i++) {
					File file = new File(datedFilePaths[i]);
					SystemClassPathManager.addFile(file);
				}
				
				// Register KeyTypes
				for (int i = 0; i < datedFilePaths.length; i++) {
					Class[] classes = ClassFinder.getAllClasses(datedFilePaths[i]);
					for (int j = 0; j < classes.length; j++) {
						Class<?> cls = classes[j];
						if (KeyType.class.isAssignableFrom(cls) && 
							cls.getSimpleName().matches(".*_v\\d++$")) 
						{
							Method method = cls.getMethod("getKeyType", (Class[])null);
							KeyType fieldType = (KeyType)method.invoke(cls, (Object[])null);
							KeyTypeManager.registerSingleKeyType(fieldType);
						}
					}
				}
				
				codeMessage = "deployed to " + classDirFile.getAbsolutePath();
			} catch (Exception ex) {
				while (ex.getCause() != null) {
					ex = (Exception)ex.getCause();
				}
				codeMessage = ex.getMessage();
				if (codeMessage != null) 
					codeMessage = codeMessage.trim();
				if (codeMessage == null || codeMessage.length() == 0) {
					codeMessage = ex.getClass().getSimpleName();
				}
			}
			
			data.setDataObject(codeMessage);
		}
		
		return data;
	}
	
	private static String getDatedJarName(String jarName)
	{
		String nameNoExtension = jarName.substring(0, jarName.lastIndexOf(".jar"));
		return nameNoExtension + ".v" + dateFormatter.format(new Date()) + ".jar";
	}

	public byte getCode()
	{
		return code;
	}
	
	public String getCodeMessage()
	{
		return codeMessage;
	}
}
