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
	superclass ::pgwire ::tdbc::connection

	variable {*}{
		host
		port
		db
		user
		password

		socket
		transaction_status
		ready_for_query
	}

	constructor args { #<<<
		if {[llength $args] == 2 && [lindex $args 0] eq "-attach"} {
			next {*}$args
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

			next

			my _reconnect
		}
	}

	#>>>
	method detach {} { #<<<
		foreach stmt [my statements] {
			$stmt destroy
		}
		next
	}

	#>>>

	method _connection_lost {} { #<<<
		if {$transaction_status eq "I"} {
			# Attempt a reconnect
			close $socket
			unset socket
			my _reconnect
		} else {
			next
		}
	}

	#>>>
	method _reconnect {} { #<<<
		if {[info exists socket] && $socket in [chan names]} {
			error "Already connected"
		}
		set chan	[socket $host $port]

		try {
			my connect $chan $db $user $password
		} on error {errmsg options} {
			if {$chan in [chan names]} {
				close $chan
			}
			return -options $options $errmsg
		}
	}

	#>>>
	method primarykeys tableName { #<<<
		tailcall my allrows {
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

		my allrows {
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
		makerow_list
	}

	constructor {instance sqlcode} { #<<<
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
			1 {lassign $args dict}
			default {
				return -code error -errorcode [concat $generalError wrongNumArgs] \
						"wrong # args: should be [lrange [info level 0] 0 1]\
						 ?-option value?... ?--? ?dictionary?"
			}
		}

		#puts stderr "execute_lambda:\n[join [lmap line [split $execute_lambda \n] {format {%3d: %s} [incr lineno] $line}] \n]"
		if {[info exists dict]} {
			coroutine portal apply $execute_lambda $socket "" 0 $dict
		} else {
			uplevel 1 [list coroutine [namespace current]::portal apply $execute_lambda $socket "" 0]
		}

		try {
			switch -exact -- [dict get $opts -as] {
				dicts {
					while 1 {
						set msg	[portal]
						switch -exact -- [lindex $msg 0] {
							DataRow {
								try $makerow_dict
								if {[eof $socket]} {unset socket; $con connection_lost}
								lappend acc $row
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
								try $makerow_list
								if {[eof $socket]} {unset socket; $con connection_lost}
								lappend acc $row
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
			3 {lassign $args row_varname script}
			4 {lassign $args row_varname dict script}
			default {
				return -code error -errorcode [concat $generalError wrongNumArgs] \
						"wrong # args: should be [lrange [info level 0] 0 1]\
						 ?-option value?... ?--? varName ?dictionary? script"
			}
		}

		if {[info exists dict]} {
			coroutine portal apply $execute_lambda $socket "" 0 $dict
		} else {
			uplevel 1 [list coroutine [namespace current]::portal apply $execute_lambda $socket "" 0]
		}

		upvar 1 $row_varname row

		try {
			switch -exact -- [dict get $opts -as] {
				dicts {
					while 1 {
						set msg	[portal]
						switch -exact -- [lindex $msg 0] {
							DataRow {
								try $makerow_dict
								if {[eof $socket]} {unset socket; $con connection_lost}
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
								try $makerow_list
								if {[eof $socket]} {unset socket; $con connection_lost}
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
}

#>>>
oo::class create ::tdbc::pgwire::resultset { #<<<
	constructor {stmt args} { #<<<
	}

	#>>>
}

#>>>

# vim: foldmethod=marker foldmarker=<<<,>>> ts=4 shiftwidth=4
