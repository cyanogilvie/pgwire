proc make_pgwire_lo {} { #<<<
	if {[llength [info commands pgwire_lo]] == 0} {
		pgwire create pgwire_lo [socket db 5432] pagila postgres insecure
	}
}

#>>>
proc make_pgwire_uds {} { #<<<
	if {[llength [info commands pgwire_uds]] == 0} {
		package require unix_sockets
		pgwire create pgwire_uds [unix_sockets::connect [file join $::env(PGHOST) .s.PGSQL.5432]] pagila postgres insecure
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
