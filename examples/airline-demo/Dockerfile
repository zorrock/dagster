# Dockerfile for Airflow

FROM python:3.7.2-stretch

# We need to use java 8 because https://stackoverflow.com/questions/53583199/pyspark-error-unsupported-class-file-major-version-55
##################################################################################################
# https://github.com/docker-library/openjdk/blob/c3023e4da10d10e9c9775eabe2d7baac146e7ae1/8/jdk/slim/Dockerfile

# A few reasons for installing distribution-provided OpenJDK:
#
#  1. Oracle.  Licensing prevents us from redistributing the official JDK.
#
#  2. Compiling OpenJDK also requires the JDK to be installed, and it gets
#     really hairy.
#
#     For some sample build times, see Debian's buildd logs:
#       https://buildd.debian.org/status/logs.php?pkg=openjdk-8

RUN apt-get update && apt-get install -y --no-install-recommends \
	bzip2 \
	unzip \
	xz-utils \
	&& rm -rf /var/lib/apt/lists/*

# Default to UTF-8 file.encoding
ENV LANG C.UTF-8

# add a simple script that can auto-detect the appropriate JAVA_HOME value
# based on whether the JDK or only the JRE is installed
RUN { \
	echo '#!/bin/sh'; \
	echo 'set -e'; \
	echo; \
	echo 'dirname "$(dirname "$(readlink -f "$(which javac || which java)")")"'; \
	} > /usr/local/bin/docker-java-home \
	&& chmod +x /usr/local/bin/docker-java-home

# do some fancy footwork to create a JAVA_HOME that's cross-architecture-safe
RUN ln -svT "/usr/lib/jvm/java-8-openjdk-$(dpkg --print-architecture)" /docker-java-home
ENV JAVA_HOME /docker-java-home

# Updated from base image
ENV JAVA_VERSION 8u212
ENV JAVA_DEBIAN_VERSION 8u212-b01-1~deb9u1 

RUN set -ex; \
	# deal with slim variants not having man page directories (which causes "update-alternatives" to fail)
	if [ ! -d /usr/share/man/man1 ]; then \
	mkdir -p /usr/share/man/man1; \
	fi; \
	apt-get update; \
	apt-get install -y --no-install-recommends \
	openjdk-8-jdk-headless="$JAVA_DEBIAN_VERSION" \
	; \
	rm -rf /var/lib/apt/lists/*; \
	# verify that "docker-java-home" returns what we expect
	[ "$(readlink -f "$JAVA_HOME")" = "$(docker-java-home)" ]; \
	# update-alternatives so that future installs of other OpenJDK versions don't change /usr/bin/java
	update-alternatives --get-selections | awk -v home="$(readlink -f "$JAVA_HOME")" 'index($3, home) == 1 { $2 = "manual"; print | "update-alternatives --set-selections" }'; \
	# ... and verify that it actually worked for one of the alternatives we care about
	update-alternatives --query java | grep -q 'Status: manual'

##################################################################################################

RUN mkdir -p /tmp/results

WORKDIR /tmp/

ADD dagster/requirements.txt dagster_requirements.txt
RUN pip install -r dagster_requirements.txt

ADD dagster-graphql/requirements.txt dagster_graphql_requirements.txt
RUN pip install -r dagster_graphql_requirements.txt

ADD dagstermill/requirements.txt dagstermill_requirements.txt
RUN pip install -r dagstermill_requirements.txt

ADD requirements.txt .

RUN pip install -r requirements.txt

ADD dagster dagster
ADD dagster-graphql dagster-graphql
ADD dagstermill dagstermill

RUN pip install -e dagster/
RUN pip install -e dagster-graphql/
RUN pip install -e dagstermill/

ADD . .

RUN pip install -e .

RUN python -m airline_demo.resources

ENTRYPOINT [ "dagster-graphql" ]

EXPOSE 3000
