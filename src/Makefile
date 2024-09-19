# First two numbers are the PostgreSQL protocol version number, rest is ours
VER = 3.0.0b28
TCLSH = tclsh
VALGRINDEXTRA =
VALGRINDARGS = --tool=memcheck --num-callers=8 --leak-resolution=high \
			   --leak-check=yes -v --suppressions=suppressions --keep-debuginfo=yes \
			   --trace-children=no --error-exitcode=2 $(VALGRINDEXTRA)

all: tm

tm: tm/pgwire-$(VER).tm tm/tdbc/pgwire-$(VER).tm

tm/pgwire-$(VER).tm: pgwire.tcl *.c
	mkdir -p tm
	$(TCLSH) inline_c.tcl pgwire.tcl tm/pgwire-$(VER).tm

tm/tdbc/pgwire-$(VER).tm: tdbcpgwire.tcl
	mkdir -p tm/tdbc
	cp tdbcpgwire.tcl tm/tdbc/pgwire-$(VER).tm

gdb: all
	gdb -ex=r --args tclsh tests/all.tcl $(VER) $(TESTFLAGS)

test: all
	AWS_PROFILE="$(AWS_PROFILE)" $(TCLSH) tests/all.tcl $(VER) $(TESTFLAGS)

valgrind: all
	AWS_PROFILE="$(AWS_PROFILE)" valgrind $(VALGRINDARGS) $(TCLSH) tests/all.tcl $(VER) $(TESTFLAGS)

benchmark: all
	#tc qdisc add dev eth0 root netem delay .5ms
	nice -n -20 taskset -c 4-7 $(TCLSH) bench/run.tcl $(VER) $(TESTFLAGS)
	#tc qdisc del dev eth0 root netem

vim-gdb: all
	vim -c "set number" -c "set mouse=a" -c "set foldlevel=100" -c "Termdebug -ex set\ print\ pretty\ on --args $(TCLSH) tests/all.tcl $(VER) -singleproc 1 $(TESTFLAGS)" -c "2windo set nonumber" -c "1windo set nonumber" accel.c

gdb_benchmark: all
	gdb -ex=r --args $(TCLSH) bench/run.tcl $(VER) $(TESTFLAGS)

clean:
	-rm -rf tm/*

.PHONY: tm all gdb test valgrind benchmark gdb_benchmark clean vim-gdb
