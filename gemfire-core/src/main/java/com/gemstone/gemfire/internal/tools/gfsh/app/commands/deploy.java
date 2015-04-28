package com.gemstone.gemfire.internal.tools.gfsh.app.commands;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.data.MemberInfo;
import com.gemstone.gemfire.internal.tools.gfsh.app.function.GfshData;
import com.gemstone.gemfire.internal.tools.gfsh.app.function.GfshFunction;

public class deploy implements CommandExecutable
{
	private Gfsh gfsh;
	
	private static int BUFFER_SIZE = 10000;
	
	public deploy(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	public void help()
	{
		gfsh.println("deploy [-jar <jar paths>]");
		gfsh.println("       [-dir [-r] <directory>]");
		gfsh.println("       [-?]");
		gfsh.println("   Deploys the specified jar or class files to all of the servers.");
		gfsh.println("     -jar <jar paths> Load all classes in the jar paths. The jar paths");
		gfsh.println("            can be separated by ',', ';', or ':'. The jar paths");
		gfsh.println("            can be relative or absolute.");
		gfsh.println("     -dir [-r] <directory> Load all jar files in the directory.");
		gfsh.println("            '-r' recursively loads all jar files including sub-directories.");
		gfsh.println("            The directory path can be relative or absolute.");
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("deploy -?")) {
			help();
		} else if (command.startsWith("deploy -dir")) {
			deploy_dir(command);
		} else if (command.startsWith("deploy -jar")) {
			deploy_jar(command);
		} else {
			gfsh.println("Error: invalid command - " + command);
		}
	}
	
	private byte[] readJar(File file) throws FileNotFoundException, IOException
	{
		FileInputStream fis = new FileInputStream(file);
		ArrayList<byte[]> byteList = new ArrayList<byte[]>();
		int bytesRead;
		int lastBytesRead = 0;
		byte buffer[];
		do {
			buffer = new byte[BUFFER_SIZE];
			bytesRead = fis.read(buffer);
			if (bytesRead != -1) {
				lastBytesRead = bytesRead;
				byteList.add(buffer);
			}
		} while (bytesRead != -1);
		fis.close();
		
		int lastIndex = byteList.size() - 1;
		int bufferLength = lastIndex *  BUFFER_SIZE + lastBytesRead;
		int destPos = 0;
		buffer = new byte[bufferLength];
		for (int j = 0; j < lastIndex; j++) {
			byte srcBuffer[] = byteList.get(j);
			destPos = j * BUFFER_SIZE;
			System.arraycopy(srcBuffer, 0, buffer, destPos, srcBuffer.length);
		}
		if (lastIndex >= 0) {
			byte srcBuffer[] = byteList.get(lastIndex);
			destPos = lastIndex * BUFFER_SIZE;
			System.arraycopy(srcBuffer, 0, buffer, destPos, lastBytesRead);
		}
		
		return buffer;
	}
	
	private void deploy(String[] jarPaths)
	{
		if (jarPaths == null || jarPaths.length == 0) {
			gfsh.println("Error: must specify the jar path(s)");
			return;
		}
		
		try {
			String jarNames[] = new String[jarPaths.length];
			byte[][] payloadBuffers = new byte[jarPaths.length][];			
			
			// Read all jar files
			for (int i = 0; i < jarPaths.length; i++) {
				String path = jarPaths[i];
				if (path == null) {
					continue;
				}
				path = path.trim();
				File file = new File(path);
				jarNames[i] = file.getName();
				payloadBuffers[i] = readJar(file);
			}
			
			// Send payloadBuffers to all servers
			long startTime = System.currentTimeMillis();
			List<AggregateResults> results = (List<AggregateResults>) gfsh.getAggregator().aggregate(
					new GfshFunction("deploy", null, new Object[] { "-jar", jarNames, payloadBuffers }), gfsh.getAggregateRegionPath());
			long stopTime = System.currentTimeMillis();
			
			int i = 1;
			for (AggregateResults aggregateResults : results) {
				GfshData data = (GfshData)aggregateResults.getDataObject();
				MemberInfo memberInfo = data.getMemberInfo();
				String message = (String)data.getDataObject();
				gfsh.print(i + ". " + memberInfo.getMemberName() + "(" + memberInfo.getMemberId() + ")" + ": ");
				gfsh.println(message);
				i++;
			}
			
			gfsh.print("deployed files: ");
			for (i = 0; i < jarNames.length - 1; i++) {
				gfsh.print(jarNames[i] + ", ");
			}
			if (jarNames.length > 0) {
				gfsh.println(jarNames[jarNames.length - 1]);
			}
			gfsh.println();
			
			if (gfsh.isShowTime()) {
				gfsh.println("elapsed (msec): " + (stopTime - startTime));
			}
			
		} catch (Exception ex) {
			gfsh.println("Error: " + ex.getClass().getSimpleName() + " - " + ex.getMessage());
			if (gfsh.isDebug()) {
				ex.printStackTrace();
			}
		}
	}

	private void deploy_jar(String command)
	{
		ArrayList list = new ArrayList();
		gfsh.parseCommand(command, list);
		if (list.size() < 3) {
			gfsh.println("Error: must specify the jar path(s)");
			return;
		}

		String jarPaths = "";
		for (int i = 2; i < list.size(); i++) {
			jarPaths += list.get(i);
		}
		jarPaths = jarPaths.trim();

		String pathSeparator = System.getProperty("path.separator");
		jarPaths = jarPaths.replace(pathSeparator.charAt(0), ',');
		String split[] = jarPaths.split(",");
		deploy(split);
	}	
	
	private void deploy_dir(String command)
	{
		ArrayList list = new ArrayList();
		gfsh.parseCommand(command, list);
		if (list.size() < 3) {
			gfsh.println("Error: must specify the directory path");
			return;
		}
		
		String dirPath = (String)list.get(2);
		if (dirPath == null) {
			return;
		}
		dirPath = dirPath.trim();
		if (dirPath.length() == 0) {
			gfsh.println("Error: directory not specified");
			return;
		}
		
		File file = new File(dirPath);
		if (file.exists() == false) {
			gfsh.println("Error: direcotry " + dirPath + " does not exist");
			return;
		}
		
		ArrayList<File> jarFileList = getJarFiles(file, false);
		if (jarFileList.size() == 0) {
			gfsh.println("jar files not found in directory " + dirPath);
			return;
		}
		
		String jarPaths[] = new String[jarFileList.size()];
		int i = 0;
		for (File file2 : jarFileList) {
			jarPaths[i++] = file2.getAbsolutePath();
		}
		deploy(jarPaths);
		
//		byte[][] payloadBuffers = new byte[jarFileList.size()][];		
//		try {
//		
//			// Read all jar files
//			for (int i = 0; i < jarFileList.size(); i++) {
//				File file2 = jarFileList.get(i);
//				payloadBuffers[i] = readJar(file2);
//			}
//			
//			// Determine the jar names
//			for (int i = 0; i < jarFileList.size(); i++) {
//				File file2 = jarFileList.get(i);
//				jarPaths[i] = file2.getAbsolutePath();
//			}
//			
//			deploy(jarPaths);
//			
//		} catch (Exception ex) {
//			gfsh.println("Error: " + ex.getClass().getSimpleName() + " - " + ex.getMessage());
//			if (gfsh.isDebug()) {
//				ex.printStackTrace();
//			}
//		}
	}
	
	private ArrayList<File> getJarFiles(File dir, boolean recursive)
	{
		return getJarFiles(dir, new ArrayList<File>(), recursive);
	}
	
	private ArrayList<File> getJarFiles(File dir, ArrayList<File> fileList, boolean recursive)
	{
		File files[] = dir.listFiles(new FileFilter() {

			public boolean accept(File pathname)
			{
				return (pathname.isDirectory() == false && pathname.getName().endsWith(".jar"));
			}
		});
		
		for (int i = 0; i < files.length; i++) {
			fileList.add(files[i]);
		}
		
		if (recursive) {
			File dirs[] = dir.listFiles(new FileFilter() {
	
				public boolean accept(File pathname)
				{
					return pathname.isDirectory();
				}
			});
			
			for (int i = 0; i < dirs.length; i++) {
				fileList = getJarFiles(dirs[i], fileList, recursive);
			}
		}
		
		return fileList;
	}
}
