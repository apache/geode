FROM centos:latest
MAINTAINER William Markito <william.markito@gmail.com>

LABEL Vendor="Apache Geode (incubating)"
LABEL version=unstable

# download JDK 8
ENV	JAVA_HOME /jdk1.8.0_51

RUN	yum install -y wget which tar git \
	&& wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u51-b16/jdk-8u51-linux-x64.tar.gz" \
	&& tar xf jdk-8u51-linux-x64.tar.gz \
	&& git clone -b develop https://github.com/apache/incubator-geode.git \
	&& cd incubator-geode \
	&& ./gradlew build -Dskip.tests=true \
	&& ls /incubator-geode | grep -v gemfire-assembly | xargs rm -rf \
	&& rm -rf /root/.gradle/ \
	&& rm -rf /incubator-geode/gemfire-assembly/build/distributions/ \
	&& rm -rf /jdk-8u51-linux-x64.tar.gz \
	&& rm -rf $JAVA_HOME/*src.zip \
						$JAVA_HOME/lib/missioncontrol \
						$JAVA_HOME/lib/visualvm \
						$JAVA_HOME/lib/*javafx* \
						$JAVA_HOME/jre/lib/plugin.jar \
						$JAVA_HOME/jre/lib/ext/jfxrt.jar \
						$JAVA_HOME/jre/bin/javaws \
						$JAVA_HOME/jre/lib/javaws.jar \
						$JAVA_HOME/jre/lib/desktop \
						$JAVA_HOME/jre/plugin \
						$JAVA_HOME/jre/lib/deploy* \
						$JAVA_HOME/jre/lib/*javafx* \
						$JAVA_HOME/jre/lib/*jfx* \
						$JAVA_HOME/jre/lib/amd64/libdecora_sse.so \
						$JAVA_HOME/jre/lib/amd64/libprism_*.so \
						$JAVA_HOME/jre/lib/amd64/libfxplugins.so \
						$JAVA_HOME/jre/lib/amd64/libglass.so \
						$JAVA_HOME/jre/lib/amd64/libgstreamer-lite.so \
						$JAVA_HOME/jre/lib/amd64/libjavafx*.so \
						$JAVA_HOME/jre/lib/amd64/libjfx*.so \
	&& rm -rf /usr/share/locale/* \
	&& yum remove -y perl \
	&& yum clean all

ENV GEODE_HOME /incubator-geode/gemfire-assembly/build/install/apache-geode
ENV PATH $PATH:$GEODE_HOME/bin:$JAVA_HOME/bin

# Default ports:
# RMI/JMX 1099
# REST 8080
# PULE 7070
# LOCATOR 10334
# CACHESERVER 40404
EXPOSE  8080 10334 40404 1099 7070
VOLUME ["/data/"]
CMD ["gfsh"]
#ENTRYPOINT ["gfsh"]
