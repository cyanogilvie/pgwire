FROM cyanogilvie/tcl:8.7pre1
WORKDIR /src
RUN apk --update --no-cache add make postgresql-client gdb
ENTRYPOINT ["/usr/bin/make"]
CMD ["test"]
