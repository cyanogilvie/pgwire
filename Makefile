all:
	make -C src

test: all
	docker-compose run --rm pgwire test TESTFLAGS="$(TESTFLAGS)"

gdb: all
	docker-compose run --rm pgwire gdb TESTFLAGS="$(TESTFLAGS)"

benchmark: all
	docker-compose up -d db
	docker-compose exec db tc qdisc add dev eth0 root netem delay .5ms
	docker-compose run --rm pgwire benchmark TESTFLAGS="$(TESTFLAGS)"
	docker-compose exec db tc qdisc del dev eth0 root netem

gdb_benchmark: all
	docker-compose run --rm pgwire gdb_benchmark TESTFLAGS="$(TESTFLAGS)"

clean:
	make -C src clean
	docker-compose down --rmi all -v --remove-orphans
