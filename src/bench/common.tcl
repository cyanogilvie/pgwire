variable base	[file dirname [file dirname [file normalize [info script]]]]
variable PGHOST	[if {[info exists ::env(PGHOST)]} {set ::env(PGHOST)} {return -level 0 db}]
variable PGPORT	[if {[info exists ::env(PGPORT)]} {set ::env(PGPORT)} {return -level 0 5432}]

proc readfile fn { #<<<
	set h	[open $fn r]
	try {read $h} finally {close $h}
}

#>>>
proc make_pgwire_lo {} { #<<<
	variable PGHOST
	variable PGPORT
	if {[llength [info commands pgwire_lo]] == 0} {
		pgwire create pgwire_lo [socket $PGHOST $PGPORT] pagila postgres insecure
	}
}

#>>>
proc make_pgwire_uds {} { #<<<
	if {[llength [info commands pgwire_uds]] == 0} {
		package require unix_sockets
		pgwire create pgwire_uds [unix_sockets::connect [file join /run/postgresql/.s.PGSQL.5432]] pagila postgres insecure
	}
}

#>>>
proc make_tdbc_postgres {} { #<<<
	if {[llength [info commands tdbc_postgres]] == 0} {
		package require tdbc::postgres
		tdbc::postgres::connection create tdbc_postgres -host db -db pagila -user postgres -password insecure
	}
}

#>>>
proc save_acc {} { #<<<
	variable acc	$::pgwire::accelerators
}

#>>>
proc restore_acc {} { #<<<
	variable acc
	set ::pgwire::accelerators	$acc
}

#>>>
proc disable_acc {} { #<<<
	set ::pgwire::accelerators	0
}

#>>>
proc try_force_acc {} { #<<<
	variable acc
	if {!($acc)} {error "Accelerators not available"}
	set ::pgwire::accelerators	1
}

#>>>

# vim: ft=tcl foldmethod=marker foldmarker=<<<,>>> ts=4 shiftwidth=4
