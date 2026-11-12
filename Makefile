DC=docker compose
# --build: rebuilt (from the layer cache) when PG_VERSION, the platform or a Dockerfile has changed
RUN=$(DC) run --build --rm

all:
	make -C src all

test: all
	$(RUN) pgwire test TESTFLAGS="$(TESTFLAGS)" AWS_PROFILE="$(AWS_PROFILE)"

tcpdump: all
	$(RUN) -e TCPDUMP="/tmp/out/tcldump_" pgwire test TESTFLAGS="$(TESTFLAGS)" AWS_PROFILE="$(AWS_PROFILE)"

valgrind: all
	$(RUN) pgwire valgrind TESTFLAGS="$(TESTFLAGS)" AWS_PROFILE="$(AWS_PROFILE)"

gdb: all
	$(RUN) pgwire gdb TESTFLAGS="$(TESTFLAGS)" AWS_PROFILE="$(AWS_PROFILE)"

benchmark: all
	$(DC) up -d --build db
	#-$(DC) exec db tc qdisc del dev eth0 root netem
	#$(DC) exec db tc qdisc add dev eth0 root netem delay .16ms
	$(RUN) pgwire benchmark TESTFLAGS="$(TESTFLAGS)"
	#$(DC) exec db tc qdisc del dev eth0 root netem

gdb_benchmark: all
	$(RUN) pgwire gdb_benchmark TESTFLAGS="$(TESTFLAGS)"

clean:
	make -C src clean
	$(DC) down --rmi all -v --remove-orphans
