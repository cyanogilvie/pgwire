if {0 && [namespace exists ::tdbc::pgwire]} {
	if {[info object isa object ::tdbc::pgwire::connection]} {
		rename ::tdbc::pgwire::connection {}
	}
	if {[info object isa object ::tdbc::pgwire::statement]} {
		rename ::tdbc::pgwire::statement {}
	}
	if {[info object isa object ::tdbc::pgwire::resultset]} {
		rename ::tdbc::pgwire::resultset {}
	}
	#namespace delete ::tdbc::pgwire
}

package require tdbc
package require pgwire

namespace eval ::tdbc::pgwire {
	namespace export connection
}

oo::class create ::tdbc::pgwire::connection { #<<<
	superclass ::tdbc::connection

	variable {*}{
		host
		port
		db
		user
		password

		socket
	}

	constructor args { #<<<
		if {[llength $args] == 2 && [lindex $args 0] eq "-attach"} {
			::pgwire create pg {*}$args
		} else {
			if {![catch {package require parse_args}]} {
				parse_args::parse_args $args {
					-host		{-default localhost}
					-port		{-default 5432}
					-user		{-required}
					-password	{-required}
					-db			{-default template1}
				}
			} else {
				if {[llength $args] % 2 != 0} {
					error "Wrong number of args, must be pairs of -arg val"
				}
				set port	5432
				set host	localhost
				set db		template1
				foreach {k v} $args {
					switch -exact -- $k {
						-host		{set host $v}
						-port		{set port $v}
						-user		{set user $v}
						-password	{set password $v}
						-db			{set db $v}
						default {
							error "Invalid option \"$k\", must be one of -host, -port, -user, -password, -db"
						}
					}
				}
				foreach v {user password} {
					if {![info exists $v]} {
						error "Option -$v is required"
					}
				}
			}

			::pgwire create pg

			my _reconnect
		}
	}

	#>>>
	destructor { #<<<
		foreach stmt [my statements] {
			$stmt destroy
		}
		if {[self next] ne ""} next
	}

	#>>>
	method detach {} { #<<<
		if {[info object isa object pg]} {
			puts stderr "tdbc::pgwire::detach, detaching pg"
			foreach stmt [my statements] {
				$stmt destroy
			}
			pg detach
		}
	}

	#>>>

	method _reconnect {} { #<<<
		if {[info exists socket] && $socket in [chan names]} {
			error "Already connected"
		}
		set socket	[socket $host $port]

		try {
			pg connect $socket $db $user $password
		} on error {errmsg options} {
			if {$socket in [chan names]} {
				close $socket
			}
			return -options $options $errmsg
		}
	}

	#>>>
	method primarykeys tableName { #<<<
		tailcall pg allrows {
			select
				xtable.table_schema			as tableSchema,
				xtable.table_name			as tableName,
				xtable.constraint_catalog	as constraintCatalog,
				xtable.constraint_schema	as constraintSchema,
				xtable.constraint_name		as constraintName,
				xcolumn.column_name			as columnName,
				xcolumn.ordinal_position	as ordinalPosition
			from
				information_schema.table_constraints	xtable
			inner join
				information_schema.key_column_usage		xcolumn
			on
				xtable.constraint_schema = xcolumn.constraint_schema and
				xtable.table_name = xcolumn.table_name and
				xtable.constraint_name = xcolumn.constraint_name and
				xtable.constraint_catalog = xcolumn.constraint_catalog
			where
				xtable.table_name = :tableName and
				xtable.constraint_type = 'PRIMARY KEY'
		}
	}

	#>>>
	method foreignkeys args { #<<<
		variable ::tdbc::generalError

		if {![catch {package require parse_args}]} {
			parse_args::parse_args $args {
				-primary	{}
				-foreign	{}
			}
		} else {
			set argdict {}
			if {[llength $args] % 2 != 0} {
				throw [list {*}$generalError wrongNumArgs] "wrong # args: should be [lrange [info level 0] 0 1] ?-option value?..."
			}
			foreach {k v} {
				switch -exact -- $k {
					-primary {set primary $v}
					-foreign {set foreign $v}
					default {
						throw [list {*}$generalError badOption] "bad option \"$key\", must be -primary or -foreign"
					}
				}
			}
		}

		pg allrows {
			select
				rc.constraint_catalog			as foreignConstraintCatalog,
				rc.constraint_schema			as foreignConstraintSchema,
				rc.constraint_name				as foreignConstraintName,
				rc.unique_constraint_catalog	as primaryConstraintCatalog,
				rc.unique_constraint_schema		as primaryConstraintSchema,
				rc.unique_constraint_name		as primaryConstraintName,
				rc.update_rule					as updateAction,
				rc.delete_rule					as deleteAction,
				pkc.table_catalog				as primaryCatalog,
				pkc.table_schema				as primarySchema,
				pkc.table_name					as primaryTable,
				pkc.column_name					as primaryColumn,
				fkc.table_catalog				as foreignCatalog,
				fkc.table_schema				as foreignSchema,
				fkc.table_name					as foreignTable,
				fkc.column_name					as foreignColumn,
				pkc.ordinal_position			as ordinalPosition
			from
				information_schema.referential_constraints rc
			inner join
				information_schema.key_column_usage fkc
			on
				fkc.constraint_name = rc.constraint_name and
				fkc.constraint_schema = rc.constraint_schema and
				fkc.constraint_catalog = rc.constraint_catalog
			inner join
				information_schema.key_column_usage pkc
			on
				pkc.constraint_name = rc.unique_constraint_name and
				pkc.constraint_schema = rc.unique_constraint_schema and
				pkc.constraint_catalog = rc.constraint_catalog and
				pkc.ordinal_position = fkc.ordinal_position
			[if {[info exists primary] || [info exists foreign]} {
				set terms	{}
				if {[info exists primary]} {lappend terms {pkc.table_name = :primary}}
				if {[info exists foreign]} {lappend terms {fkc.table_name = :foreign}}
				subst {where
				[join $terms { and
				}]}}]
			order by
				foreignConstraintCatalog,
				foreignConstraintSchema,
				foreignConstraintName,
				ordinalPosition
		}
	}

	#>>>

	forward statementCreate ::tdbc::pgwire::statement create
	forward begintransaction	pg begintransaction
	forward rollback			pg rollback
	forward commit				pg commit
	forward allrows				pg allrows
	forward foreach				pg foreach
	forward onecolumn			pg onecolumn
	forward prepare_statement	pg prepare_statement
	forward close_statement		pg close_statement
	forward extended_query		pg extended_query
	forward skip_to_sync		pg skip_to_sync
}

#>>>
oo::class create ::tdbc::pgwire::statement { #<<<
	superclass ::tdbc::statement

	variable {*}{
		con

		socket
		field_names
		execute_lambda
		makerow_vars
		makerow_dict
		makerow_dict_no_nulls
		makerow_list
		build_params
	}

	constructor {instance sqlcode} { #<<<
		if {"::tcl::mathop" ni [namespace path]} {
			namespace path [list {*}[namespace path] {*}{
				::tcl::mathop
			}]
		}
		set con	$instance

		if {[self next] ne ""} next

		set stmt_info	[$con prepare_statement [self] $sqlcode]
		dict with stmt_info {}
	}

	#>>>
	destructor { #<<<
		$con close_statement [self]

		# Closing a statement implicitly closes any resultsets derived from it
		foreach resultset [my resultsets] {
			# These will each issue a close for their portals, but it isn't an error to close a non-existant portal
			$resultset destroy
		}

		if {[self next] ne ""} next
	}

	#>>>
	method allrows args { #<<<
		variable ::tdbc::generalError

		set args	[::tdbc::ParseConvenienceArgs $args opts]
		switch -exact -- [llength $args] {
			0 {}
			1 {lassign $args param_values}
			default {
				return -code error -errorcode [concat $generalError wrongNumArgs] \
						"wrong # args: should be [lrange [info level 0] 0 1]\
						 ?-option value?... ?--? ?dictionary?"
			}
		}

		try $build_params

		set max_rows_per_batch	0
		#puts stderr "execute_lambda:\n[join [lmap line [split $execute_lambda \n] {format {%3d: %s} [incr lineno] $line}] \n]"
		lassign [coroutine portal apply $execute_lambda $socket "" $max_rows_per_batch 1 $parameters] \
			columns rformats tcl_encoding c_types

		try {
			set acc	{}
			switch -exact -- [dict get $opts -as] {
				dicts {
					while 1 {
						set msg	[portal]
						#::pgwire::log notice "portal yielded: $msg"
						switch -exact -- [lindex $msg 0] {
							DataRow {
								set data	[read $socket [lindex $msg 1]]
								if {[eof $socket]} {unset socket; $con connection_lost}
								if {$::pgwire::accelerators} {
									set row	[::pgwire::c_makerow $data $c_types $rformats $tcl_encoding dicts]
								} else $makerow_dict
								lappend acc $row
							}
							PortalSuspended {
								set executemsg	[lindex $msg 1]
								# Execute portal
								puts -nonewline $socket [binary format aIa*I E [+ 8 [string length $executemsg]] $executemsg $max_rows_per_batch 0 {*}$args]H\u0\u0\u0\u4
								flush $socket
							}
							CommandComplete {
								set msg	[lindex $msg 1]
								#::pgwire::log notice "Got CommandComplete: \"$msg\""
							}
							finished break
							default {error "Unexpected msg \"$msg\" from portal coroutine"}
						}
					}
				}

				lists {
					while 1 {
						set msg	[portal]
						switch -exact -- [lindex $msg 0] {
							DataRow {
								set data	[read $socket [lindex $msg 1]]
								if {[eof $socket]} {unset socket; $con connection_lost}
								if {$::pgwire::accelerators} {
									set row	[::pgwire::c_makerow $data $c_types $rformats $tcl_encoding lists]
								} else $makerow_list
								lappend acc $row
							}
							PortalSuspended {
								set executemsg	[lindex $msg 1]
								# Execute portal
								puts -nonewline $socket [binary format aIa*I E [+ 8 [string length $executemsg]] $executemsg $max_rows_per_batch 0 {*}$args]H\u0\u0\u0\u4
								flush $socket
							}
							CommandComplete {
								set msg	[lindex $msg 1]
								#::pgwire::log notice "Got CommandComplete: \"$msg\""
							}
							finished break
							default {error "Unexpected msg \"$msg\" from portal coroutine"}
						}
					}
				}

				default {
					error "Unhandled -as format: \"[dict get $opts -as]\""
				}
			}

			return $acc
		} on error {r o} {
			dict incr o -level 1
		}

		if {[llength [info commands portal]] > 0} {rename portal {}}
		$con skip_to_sync
		return -options $o $r
	}

	#>>>
	method foreach args { #<<<
		variable ::tdbc::generalError

		set args	[::tdbc::ParseConvenienceArgs $args opts]
		switch -exact -- [llength $args] {
			2 {lassign $args row_varname script}
			3 {lassign $args row_varname param_values script}
			default {
				return -code error -errorcode [concat $generalError wrongNumArgs] \
						"wrong # args: should be [lrange [info level 0] 0 1]\
						 ?-option value?... ?--? varName ?dictionary? script"
			}
		}

		try $build_params

		set max_rows_per_batch	0
		#puts stderr "execute_lambda:\n[join [lmap line [split $execute_lambda \n] {format {%3d: %s} [incr lineno] $line}] \n]"
		lassign [coroutine portal apply $execute_lambda $socket "" $max_rows_per_batch 1 $parameters] \
			columns rformats tcl_encoding c_types

		upvar 1 $row_varname row

		try {
			switch -exact -- [dict get $opts -as] {
				dicts {
					while 1 {
						set msg	[portal]
						switch -exact -- [lindex $msg 0] {
							DataRow {
								set data	[read $socket [lindex $msg 1]]
								if {[eof $socket]} {unset socket; $con connection_lost}
								if {$::pgwire::accelerators} {
									set row	[::pgwire::c_makerow $data $c_types $rformats $tcl_encoding dicts]
								} else $makerow_dict
								try {
									uplevel 1 $script
								} on continue {} {
									continue
								} on break {} {
									while 1 {
										set msg	[portal]
										switch -exact -- [lindex $msg 0] {
											DataRow  - PortalSuspended - CommandComplete {
											# Read and discard messages after the break / return / error
												read $socket [lindex $msg 1]
												if {[eof $socket]} {unset socket; $con connection_lost}
											}
											finished break
											default {error "Unexpected msg \"$msg\" from portal coroutine"}
										}
									}
									break
								}
							}
							PortalSuspended {
								set executemsg	[lindex $msg 1]
								# Execute portal
								puts -nonewline $socket [binary format aIa*I E [+ 8 [string length $executemsg]] $executemsg $max_rows_per_batch 0 {*}$args]H\u0\u0\u0\u4
								flush $socket
							}
							CommandComplete {
								set msg	[lindex $msg 1]
								#::pgwire::log notice "Got CommandComplete: \"$msg\""
							}
							finished break
							default {error "Unexpected msg \"$msg\" from portal coroutine"}
						}
					}
				}

				lists {
					while 1 {
						set msg	[portal]
						switch -exact -- [lindex $msg 0] {
							DataRow {
								set data	[read $socket [lindex $msg 1]]
								if {[eof $socket]} {unset socket; $con connection_lost}
								if {$::pgwire::accelerators} {
									set row	[::pgwire::c_makerow $data $c_types $rformats $tcl_encoding dicts]
								} else $makerow_dict
								try {
									uplevel 1 $script
								} on continue {} {
									continue
								} on break {} {
									while 1 {
										set msg	[portal]
										switch -exact -- [lindex $msg 0] {
											DataRow {
											# Read and discard messages after the break / return / error
												read $socket [lindex $msg 1]
												if {[eof $socket]} {unset socket; $con connection_lost}
											}
											finished break
											default {error "Unexpected msg \"$msg\" from portal coroutine"}
										}
									}
									break
								}
							}
							PortalSuspended {
								set executemsg	[lindex $msg 1]
								# Execute portal
								puts -nonewline $socket [binary format aIa*I E [+ 8 [string length $executemsg]] $executemsg $max_rows_per_batch 0 {*}$args]H\u0\u0\u0\u4
								flush $socket
							}
							CommandComplete {
								set msg	[lindex $msg 1]
								#::pgwire::log notice "Got CommandComplete: \"$msg\""
							}
							finished break
							default {error "Unexpected msg \"$msg\" from portal coroutine"}
						}
					}
				}

				default {
					error "Unhandled -as format: \"[dict get $opts -as]\""
				}
			}
		} on ok {} {
			return
		} on return {r o} {
			dict set o -code return
			dict incr o -level 1
		} on error {r o} {
			dict incr o -level 1
		}

		if {[llength [info commands portal]] > 0} {rename portal {}}
		$con sync
		return -options $o $r
	}

	#>>>
	forward resultSetCreate ::tdbc::pgwire::resultset create
	method execute_lambda {} {set execute_lambda}
	method socket {} {set socket}
	method con {} {set con}
	method makerow_def format { set makerow_$format }
	method build_params {} { set build_params }
}

#>>>
oo::class create ::tdbc::pgwire::resultset { #<<<
	variable {*}{
		socket
		con
		batchsize
		rowdata
		execute_lambda
		open
		columns
		rformats
		tcl_encoding
		c_types
		rowcount
	}

	constructor {stmt args} { #<<<
		#::pgwire::log notice "Constructing resultset, stmt: ($stmt), args: ($args)"
		if {"::tcl::mathop" ni [namespace path]} {
			namespace path [list {*}[namespace path] {*}{
				::tcl::mathop
			}]
		}
		if {[self next] ne ""} next

		set batchsize	100
		set rowdata		{}
		set rowcount	0

		if {!$::pgwire::accelerators} {
			# No accelerator support, rewrite our nextdict and nextlist methods to use the statement-specific
			# Tcl row builder script compiled by the prepare statement step
			foreach format {dict list} {
				lassign [info class definition [self class] next$format] method_args method_body
				oo::objdefine [self] method next$format $method_args [regsub {\# makerow_start.*\# makerow_end} $method_body [string map [list \
					%makerow% [$stmt makerow_def $format] \
				] {
					%makerow%
				}]]
				#::pgwire::log notice "rewrote [self]::next$format body: [lindex [info object definition [self] next$format] 1]"
			}
			# Transcribe the variables defined to be visible in methods from the class to this instance or the
			# rewritten method won't see them
			oo::objdefine [self] variable {*}[info class variables [self class]]
		}

		set socket			[$stmt socket]
		set con				[$stmt con]
		set execute_lambda	[$stmt execute_lambda]
		try [$stmt build_params]
		lassign [coroutine [namespace current]::portal apply $execute_lambda $socket [self] $batchsize 1 $parameters] \
			columns rformats tcl_encoding c_types
		#::pgwire::log notice "Got initial data from portal:\n\t[join [lmap v {columns rformats tcl_encoding c_types} {format {%15s: %s} $v [if {$v eq "rformats"} {regexp -all -inline .. [binary encode hex [set $v]]} else {set $v}]}] \n\t]"
		set open	1
		my _read_next_batch
	}

	#>>>
	destructor { #<<<
		if {[info exists socket] && $socket in [chan names]} {
			set close_payload	P[encoding convertto $tcl_encoding [self]]\u0
			puts -nonewline $socket [binary format aIa* C [+ 4 [string length $close_payload]] $close_payload]
			flush $socket
		}
		if {[self next] ne ""} next
	}

	#>>>
	method _read_next_batch {} { #<<<
		#::pgwire::log notice "_read_next_batch"
		while 1 {
			set msg	[portal]
			#::pgwire::log notice "_read_next_batch portal returned [lindex $msg 0]"
			switch -exact -- [lindex $msg 0] {
				DataRow {
					lappend rowdata [read $socket [lindex $msg 1]]
					if {[eof $socket]} {unset socket; $con connection_lost}
				}
				PortalSuspended {
					incr rowcount	$batchsize
				}
				CommandComplete {
					#::pgwire::log notice "Got CommandComplete: \"[lindex $msg 1]\""
					incr rowcount	[lindex [lindex $msg 1] end]
					set open		0
				}
				finished {
					break
				}
				default {error "Unexpected msg \"$msg\" from portal coroutine"}
			}
		}
	}

	#>>>
	method columns {} {set columns}
	method rowcount {} {set rowcount}
	# nextdict and nextlist methods <<<
	foreach format {dict list} {
		method next$format rowvar [string map [list \
			%format%	$format \
		] {
			upvar 1 $rowvar row

			if {[llength $rowdata] == 0} {
				#::pgwire::log notice "nextdict out of data, open: $open"
				if {$open} {
					my _read_next_batch
				}
				# Re-check $open here again, _read_next_batch may have changed it
				if {!$open} {
					#::pgwire::log notice "portal closed, returning 0"
					return 0
				}
			}

			set rowdata	[lassign $rowdata data]
			#::pgwire::log notice "Popped data: [string length $data], [llength $rowdata] rows remain"
			if {[llength $rowdata] == 0 && $open} {
				#::pgwire::log notice "Drained waiting batch, still open"
				# Dispatch this here to hide some of the latency in going to the server while our caller processes this row
				lassign [coroutine [namespace current]::portal apply $execute_lambda $socket [self] $batchsize 0 {}] \
					columns rformats tcl_encoding c_types
			}

			# makerow_start
			set row	[::pgwire::c_makerow $data $c_types $rformats $tcl_encoding %format%s]
			# makerow_end
			return 1
		}]
	}

	#>>>
}

#>>>

# vim: foldmethod=marker foldmarker=<<<,>>> ts=4 shiftwidth=4
