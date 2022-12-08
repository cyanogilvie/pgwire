FROM cyanogilvie/alpine-tcl:v0.9.54-stripped
RUN apk --update --no-cache add make postgresql-client gdb iproute2 valgrind tcpdump build-base autoconf automake bsd-compat-headers bash ca-certificates libssl1.1 libcrypto1.1 docker-cli git
WORKDIR /tmp
RUN git clone --recurse-submodules --shallow-submodules --branch connection-pool-git --single-branch --depth 1 https://github.com/cyanogilvie/tdbc && \
	cd tdbc && \
	wget https://core.tcl-lang.org/tclconfig/tarball/1f17dfd726292dc4/tclconfig.tar.gz -O - | tar xz && \
    autoconf && ./configure CFLAGS="-O3" --enable-symbols && \
    make -j 8 all && \
    make install-binaries install-libraries
RUN git clone --recurse-submodules --shallow-submodules --branch detach-git --single-branch --depth 1 https://github.com/cyanogilvie/tdbcpostgres && \
	cd tdbcpostgres && \
	wget https://core.tcl-lang.org/tclconfig/tarball/1f17dfd726292dc4/tclconfig.tar.gz -O - | tar xz && \
    autoconf && ./configure CFLAGS="-O3" --enable-symbols && \
    make -j 8 all && \
    make install-binaries install-libraries
WORKDIR /src
ENTRYPOINT ["/usr/bin/make"]
CMD ["test"]
