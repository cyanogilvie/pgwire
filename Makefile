DC=docker compose

all:
	make -C src all

test: all
	$(DC) run --rm pgwire test TESTFLAGS="$(TESTFLAGS)" AWS_PROFILE="$(AWS_PROFILE)"

tcpdump: all
	$(DC) run --rm pgwire test TESTFLAGS="$(TESTFLAGS)" TCPDUMP="/tmp/testdump-" AWS_PROFILE="$(AWS_PROFILE)"

valgrind: all
	$(DC) run --rm pgwire valgrind TESTFLAGS="$(TESTFLAGS)" AWS_PROFILE="$(AWS_PROFILE)"

gdb: all
	$(DC) run --rm pgwire gdb TESTFLAGS="$(TESTFLAGS)" AWS_PROFILE="$(AWS_PROFILE)"

benchmark: all
	$(DC) up -d db
	#-$(DC) exec db tc qdisc del dev eth0 root netem
	#$(DC) exec db tc qdisc add dev eth0 root netem delay .5ms
	$(DC) run --rm pgwire benchmark TESTFLAGS="$(TESTFLAGS)"
	#$(DC) exec db tc qdisc del dev eth0 root netem

gdb_benchmark: all
	$(DC) run --rm pgwire gdb_benchmark TESTFLAGS="$(TESTFLAGS)"

clean:
	make -C src clean
	$(DC) down --rmi local -v --remove-orphans
