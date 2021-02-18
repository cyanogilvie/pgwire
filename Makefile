all:
	make -C src

test:
	docker-compose run --rm pgwire test

benchmark:
	docker-compose run --rm pgwire benchmark

clean:
	make -C src clean
	docker-compose down --rmi all -v --remove-orphans
