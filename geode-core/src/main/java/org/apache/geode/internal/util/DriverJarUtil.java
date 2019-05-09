package org.apache.geode.internal.util;

import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.DeployedJar;

import javax.sql.DataSource;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class DriverJarUtil {

    public void registerDriver(String deployedJarName)
    {
        try {
            DeployedJar jar = ClassPathLoader.getLatest().getJarDeployer().findLatestValidDeployedJarFromDisk(deployedJarName);
            String driverClassName = getJdbcDriverName(jar);
            File jarFile = jar.getFile();
            URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{jarFile.toURI().toURL()});
            Driver driver = (Driver) Class.forName(driverClassName, true,
                    urlClassLoader).newInstance();
            Driver d = new DriverWrapper(driver);
            DriverManager.registerDriver(d);
        } catch (IllegalAccessException | ClassNotFoundException | InstantiationException
                | SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    public void registerDriver(String driverClassName, String driverURL) throws MalformedURLException, SQLException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{new URL(driverURL)});
        Driver driver = (Driver) Class.forName(driverClassName, true,
                urlClassLoader).newInstance();
        Driver d = new DriverWrapper(driver);
        DriverManager.registerDriver(d);
    }


    public String getJdbcDriverName(DeployedJar jar) {
        File jarFile = jar.getFile();
        try {
            FileInputStream fileInputStream = new FileInputStream(jarFile.getAbsolutePath());
            BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
            ZipInputStream zipInputStream = new ZipInputStream(bufferedInputStream);
            ZipEntry zipEntry;
            while ((zipEntry = zipInputStream.getNextEntry()) != null) {
                // JDBC 4.0 Drivers must include the file META-INF/services/java.sql.Driver. This file
                // contains the name of the JDBC drivers implementation of java.sql.Driver
                // See https://docs.oracle.com/javase/8/docs/api/java/sql/DriverManager.html
                if (!zipEntry.getName().equals("META-INF/services/java.sql.Driver")) {
                    continue;
                }
                int size = (int) zipEntry.getSize();
                if (size == -1) {
                    throw new IOException("Invalid zip entry found for META-INF/services/java.sql.Driver " +
                            "within jar. Ensure that the jar containing the driver has been deployed and that the driver " +
                            "is at least JDBC 4.0");
                }
                byte[] bytes = new byte[size];
                int offset = 0;
                int chunk;
                while ((size - offset) > 0) {
                    chunk = zipInputStream.read(bytes, offset, size - offset);
                    if (chunk == -1) {
                        break;
                    }
                    offset += chunk;
                }
                return new String(bytes);
            }
            return null;
        } catch (IOException ex) {
            return null;
        }
    }

    // DriverManager only uses a driver loaded by system ClassLoader
    class DriverWrapper implements Driver {

        private Driver jdbcDriver;

        DriverWrapper(Driver jdbcDriver) {
            this.jdbcDriver = jdbcDriver;
        }

        public Connection connect(String url, java.util.Properties info)
                throws SQLException {
            return this.jdbcDriver.connect(url, info);
        }

        public boolean acceptsURL(String url) throws SQLException {
            return this.jdbcDriver.acceptsURL(url);
        }

        public DriverPropertyInfo[] getPropertyInfo(String url, java.util.Properties info)
                throws SQLException {
            return this.jdbcDriver.getPropertyInfo(url, info);
        }

        public int getMajorVersion() {
            return this.jdbcDriver.getMajorVersion();
        }

        public int getMinorVersion() {
            return this.jdbcDriver.getMinorVersion();
        }

        public boolean jdbcCompliant() {
            return this.jdbcDriver.jdbcCompliant();
        }

        public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return this.jdbcDriver.getParentLogger();
        }
    }
}