PGWIRE
======

This Tcl module implements the PostgreSQL wire protocol (version 3.0) in pure
Tcl script (with optional acceleration if critcl is available).  It aims to be
as reliable as possible (using only the very reliable Tcl core) but still as
fast as possible, faster than the libpq based tdbc::postgres in almost all
cases.

It acheives this performance by using every dirty trick (including extensive
metaprogramming) to produce the most efficient Tcl code it can for the critical
paths.  This unfortunately comes at the expense of code clarity and
maintainability.

~~~tcl
package require pgwire

set socket		[socket localhost 5432]
set db			postgres
set user		postgres
set password	password123
pgwire create db $socket $db $user $password

set state	something
db foreach row {
	select
		foo
	from
		bar
	where
		state = :state
} {
	puts "foo: [dict get $row foo]"
}

rename db {}
~~~

Connecting Over Unix Domain Sockets
-----------------------------------

When connecting to a database server on the local machine, PostgreSQL supports
Unix domain sockets as an alternative to the loopback address.  These offer
considerably lower overhead than the TCP stack and so are a better choice
from a performance perspective.  Tcl lacks native support for Unix domain sockets,
but an extension exists to provide them: https://github.com/cyanogilvie/unix_sockets

~~~tcl
package require unix_sockets
package require pgwire
set socket		[unix_sockets::connect /tmp/.s.PGSQL.5432]
set db			postgres
set user		postgres
set password	password123
pgwire create db $socket $db $user $password
...
~~~

Prepared Statements
-------------------

When running SQL queries using the "extended_query" method or those that are
built on it (allrows, foreach, onecolumn), the driver will create a prepared
statement and cache it for future use (improving performance for future calls
by cutting out a round-trip to the server, and allowing the server to cache
some state related to the query).  The cache is managed so as not to grow
unbounded, such that the most frequently used and most recently used prepared
statements are preserved and those that haven't been used frequently are
periodically pruned.  This is invisible at the script layer - no management
of prepared statements by the user of this module is necessary.

Connection Pooling
------------------

In a performance-critical setting like a high-volume website (such as the one
this module was built to support) the overhead of connection setup and
authentication is an unacceptible cost to pay for each hit.  This can be
addressed with a connection pooling approach whereby a handle is returned to
the pool when a thread is done with it, and when a new thread requires a handle
it retrieves a waiting one from the pool (which will create a new handle if
none are currently available).  This module supports such a scheme by providing
the ability to detach a handle from the current thread, returning a handle
which can be used to attach it to a different thread in the future.  The
connection state after a detach is guaranteed to be ready to receive a query,
and any open transaction is rolled back before the handle is detached.  Any
cached prepared statements persist with the detached handle, and can be reused
by future callers.

~~~tcl
package require pgwire
package require Thread

pgwire create db [socket localhost 5432] postgres postgres password123

set handle	[db detach]
# ... db is no longer valid ...

# ... later, possibly in a different thread ...
pgwire create db -attach $handle
# db is valid again
~~~

Performance
-----------

In these performance comparisons "tdbc_postgres" refers to the tdbc::postgres
package, but modified to support the implicit prepared statement caching
similar to pgwire.  The stock tdbc::postgres is considerably slower than the
version benchmarked here.  "pgwire_lo" refers to a connection using this
module, over the loopback (localhost) interface.  "pgwire_uds" uses the
unix_sockets extension to connect to the server over a Unix domain socket.

Times are the median value reported in microseconds, cv is the coefficient of
variation, a measure of how tightly the data cluster around the mean.  High
cv values indicate that some runs produce very different times (typically higher).

These benchmarks had the critcl accelerated support available.  The accelerator
is a fast implementation of building a row list or dictionary given the data
for row from the server.  It helps most when a very large number of column
values are returned for a query (either many rows, or many columns in each
row).  Performance for these types of queries will be slower but still usuable
if the accelerator is not available.

Running "select 1" against a database on the local machine:
~~~
-- pgwire_ns-1.1: "select 1" --------------------------------------------------
       pgwire_lo | 37.263 cv:1.0%
      pgwire_uds | 27.040 cv:0.9%
   tdbc_postgres | 57.500 cv:1.4%
~~~

Sync - a good proxy for the minimum round-trip time to the server:
~~~
-- pgwire_ns-1.4: "sync" ------------------------------------------------------
    pgwire_lo | 16.385 cv:1.3%
   pgwire_uds |  9.125 cv:1.7%
~~~

Select 500 rows from a table with 82 columns:
~~~
-- pgwire_ns-2.2: "allrows, large number of columns, dicts" -------------------
       pgwire_lo |  8814.000 cv: 3.5%
      pgwire_uds |  6908.500 cv: 1.5%
   tdbc_postgres | 13543.500 cv:14.5%

-- pgwire_ns-4.1: "foreach, large number of columns, dicts" -------------------
       pgwire_lo | 6305.500 cv: 0.7%
      pgwire_uds | 5593.500 cv: 0.4%
   tdbc_postgres | 9543.500 cv:15.1%
~~~

License
-------

Copyright 2021 Cyan Ogilve, licensed under the same terms as the Tcl Core.
