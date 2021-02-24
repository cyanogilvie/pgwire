all:
	make -C src

test: all
	docker-compose run --rm pgwire test TESTFLAGS="$(TESTFLAGS)"

benchmark: all
	docker-compose run --rm pgwire benchmark TESTFLAGS="$(TESTFLAGS)"

clean:
	make -C src clean
	docker-compose down --rmi all -v --remove-orphans
