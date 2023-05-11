# First two numbers are the PostgreSQL protocol version number, rest is ours
VER=3.0.0b22

all: tm/pgwire-$(VER).tm tm/tdbc/pgwire-$(VER).tm

tm/pgwire-$(VER).tm: pgwire.tcl
	mkdir -p tm
	cp pgwire.tcl tm/pgwire-$(VER).tm

tm/tdbc/pgwire-$(VER).tm: tdbcpgwire.tcl
	mkdir -p tm/tdbc
	cp tdbcpgwire.tcl tm/tdbc/pgwire-$(VER).tm

gdb: all
	gdb -ex=r --args tclsh tests/all.tcl $(VER) $(TESTFLAGS)

test: all
	AWS_PROFILE="$(AWS_PROFILE)" tclsh tests/all.tcl $(VER) $(TESTFLAGS)

valgrind: all
	valgrind tclsh tests/all.tcl $(VER) $(TESTFLAGS)

benchmark: all
	tc qdisc add dev eth0 root netem delay .5ms
	tclsh bench/run.tcl $(VER) $(TESTFLAGS)
	tc qdisc del dev eth0 root netem

gdb_benchmark: all
	gdb -ex=r --args tclsh bench/run.tcl $(VER) $(TESTFLAGS)

clean:
	-rm -rf tm/*
