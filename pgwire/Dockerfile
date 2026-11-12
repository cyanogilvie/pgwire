# syntax=docker/dockerfile:1
# The image the tests, benchmarks and debugging targets run in (docker-compose.yml's pgwire
# service, running src/Makefile): stock Alpine with the cftcl Tcl 9 runtime from its signed apk
# repo, for the host's arch (arm64 or x86_64), the tools the Makefile's targets use, and tdbc and
# tdbc::postgres built against cftcl: tdbc for the tdbc driver, tdbc::postgres for the benchmarks'
# comparison

ARG ALPINE_VER=3.24.4
FROM alpine:${ALPINE_VER}

# cftcl releases stay installable once superseded, so this pin rebuilds reproducibly
ARG CFTCL_VER=0.10.45
# The key fetch gets its own RUN: busybox wget's ssl_client helper can deliver a late SIGCHLD that
# interrupts the shell's next open() (EINTR)
RUN wget -qO /etc/apk/keys/cftcl.rsa.pub https://cftcl.cyanogilvie.com/alpine/v1/keys/cftcl.rsa.pub
RUN echo "v3 https://cftcl.cyanogilvie.com/alpine/v1" >> /etc/apk/repositories \
 && apk add --no-cache \
		cftcl=${CFTCL_VER} \
		cftcl-dev=${CFTCL_VER} \
		cftcl-dbg=${CFTCL_VER} \
		make \
		build-base \
		autoconf \
		automake \
		bsd-compat-headers \
		postgresql-client \
		gdb \
		valgrind \
		tcpdump \
		iproute2 \
		util-linux-misc \
		bash \
		ca-certificates \
		git \
		docker-cli \
 && ln -s /opt/cftcl/bin/tclsh /usr/local/bin/tclsh \
 && ln -s /opt/cftcl/lib/tcl9 /usr/local/lib/tcl9
# /usr/local/lib/tcl9: Tcl derives its tm path from the path it was invoked by, so `tclsh` through
# the /usr/local/bin link looks there
ENV PATH=/opt/cftcl/bin:$PATH

ARG TCLCONFIG=https://core.tcl-lang.org/tclconfig/tarball/1f17dfd726292dc4/tclconfig.tar.gz
WORKDIR /tmp
RUN git clone --recurse-submodules --shallow-submodules --branch connection-pool-git --single-branch --depth 1 https://github.com/cyanogilvie/tdbc \
 && cd tdbc \
 && wget -qO - ${TCLCONFIG} | tar xz \
 && autoconf \
 && ./configure CFLAGS="-O3" --enable-symbols --with-tcl=/opt/cftcl/lib \
 && make -j 4 all \
 && make install-binaries install-libraries \
 && cd .. && rm -rf tdbc
# tdbc::postgres from upstream (1.1.15, ported to Tcl 9): the detach-git fork isn't.  The benchmarks
# only use it as a plain connection to compare with
ARG TDBCPOSTGRES=https://core.tcl-lang.org/tdbcpostgres/tarball/47de61bf69d86fd244c0b47fc8c73d37fbfaae454d1ec2f1f3b69fdbe6cc8edf/tdbcpostgres.tar.gz
RUN wget -qO - ${TDBCPOSTGRES} | tar xz \
 && cd tdbcpostgres \
 && wget -qO - ${TCLCONFIG} | tar xz \
 && autoconf \
 && tdbc_config="$(find /opt/cftcl/lib -name tdbcConfig.sh | head -n 1)" \
 && ./configure CFLAGS="-O3" --enable-symbols --with-tcl=/opt/cftcl/lib --with-tdbc="${tdbc_config%/*}" \
 && make -j 4 all \
 && make install-binaries install-libraries \
 && cd .. && rm -rf tdbcpostgres

WORKDIR /src
ENTRYPOINT ["/usr/bin/make"]
CMD ["test"]
