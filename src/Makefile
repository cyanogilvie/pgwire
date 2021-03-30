# First two numbers are the PostgreSQL protocol version number, rest is ours
VER=3.0.0a1

all: tm/pgwire-$(VER).tm tm/tdbc/pgwire-$(VER).tm

tm/pgwire-$(VER).tm: pgwire.tcl
	mkdir -p tm
	cp pgwire.tcl tm/pgwire-$(VER).tm

tm/tdbc/pgwire-$(VER).tm: tdbcpgwire.tcl
	mkdir -p tm/tdbc
	cp tdbcpgwire.tcl tm/tdbc/pgwire-$(VER).tm

gdb: all
	gdb -ex=r --args tclsh tests/all.tcl $(TESTFLAGS)

test: all
	tclsh tests/all.tcl $(TESTFLAGS)

benchmark: all
	tc qdisc add dev eth0 root netem delay .5ms
	tclsh bench/run.tcl $(TESTFLAGS)
	tc qdisc del dev eth0 root netem

gdb_benchmark: all
	gdb -ex=r --args tclsh bench/run.tcl $(TESTFLAGS)

clean:
	-rm -rf tm/*
