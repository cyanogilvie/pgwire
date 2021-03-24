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
			#puts stderr "tdbc::pgwire::detach, detaching pg"
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
	forward prepare_extended_query	pg prepare_extended_query
	forward preserve			pg preserve
	forward release				pg release
	forward rowbuffer_coro		pg rowbuffer_coro
	forward close_statement		pg close_statement
	forward close_portal		pg close_portal
		forward extended_query		pg extended_query
	forward skip_to_sync		pg skip_to_sync
	forward tcl_encoding		pg tcl_encoding
	forward transaction_status	pg transaction_status
	forward ready_for_query		pg ready_for_query
	forward sync_outstanding	pg sync_outstanding
	forward tcl_makerow			pg tcl_makerow
}

#>>>
oo::class create ::tdbc::pgwire::statement { #<<<
	superclass ::tdbc::statement

	variable {*}{
		con
		sql

		stmt_name
		build_params
		rformats
		c_types
	}

	constructor {instance sqlcode} { #<<<
		if {"::tcl::mathop" ni [namespace path]} {
			namespace path [list {*}[namespace path] {*}{
				::tcl::mathop
			}]
		}
		set con	$instance
		set sql	$sqlcode

		if {[self next] ne ""} next

		set stmt_info	[$con prepare_extended_query $sql]
		$con preserve $sql
		dict with stmt_info {}
	}

	#>>>
	destructor { #<<<
		$con release $sql
		if {[llength [info commands [namespace current]::portal]] > 0} {
			portal destroy
		}

		foreach resultset [my resultsets] {
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
		set as	[dict get $opts -as]

		try $build_params on ok parameters {}

		if {[dict exists $opts -columnsvariable]} {
			upvar 1 [dict get $opts -columnsvariable] columns
			set columns	[dict keys $c_types]
		}

		set max_rows_per_batch	$::pgwire::default_batchsize

		if {$::pgwire::accelerators} {
			set makerow	{set row [::pgwire::c_makerow $datarow $c_types $tcl_encoding $as]}
		} else {
			set makerow	[$con tcl_makerow $as $c_types]
		}

		set tcl_encoding	[$con tcl_encoding]
		try {
			set rowbuffer	[namespace current]::portal
			coroutine $rowbuffer $con rowbuffer_coro $stmt_name $rowbuffer $rformats $parameters $max_rows_per_batch

			set rows	{}

			while 1 {
				lassign [$rowbuffer nextbatch] outcome details datarows

				foreach datarow $datarows {
					try $makerow
					lappend rows $row
				}

				switch -exact -- $outcome {
					CommandComplete -
					EmptyQueryResponse {
						break
					}
					PortalSuspended {}
					default {
						error "Unexpected outcome from rowbuffer nextbatch: \"$outcome\""
					}
				}
			}

			set rows
		} finally {
			if {[info exists rowbuffer] && [llength [info commands $rowbuffer]] > 0} {
				$rowbuffer destroy
			}
			if {![$con ready_for_query] && ![$con sync_outstanding]} {
				$con Sync
			}
			if {[$con sync_outstanding]} {
				$con skip_to_sync
			}
		}
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
		set as	[dict get $opts -as]

		try $build_params on ok parameters {}

		if {[dict exists $opts -columnsvariable]} {
			upvar 1 [dict get $opts -columnsvariable] columns
			set columns	[dict keys $c_types]
		}

		set max_rows_per_batch	$::pgwire::default_batchsize

		set rowbuffer	[namespace current]::portal
		coroutine $rowbuffer $con rowbuffer_coro $stmt_name $rowbuffer $rformats $parameters $max_rows_per_batch

		if {$::pgwire::accelerators} {
			set makerow	{set row [::pgwire::c_makerow $datarow $c_types $tcl_encoding $as]}
		} else {
			set makerow	[$con tcl_makerow $as $c_types]
		}

		set tcl_encoding	[$con tcl_encoding]
		try {
			set broken	0
			while {!$broken} {
				lassign [$rowbuffer nextbatch] outcome details datarows

				foreach datarow $datarows {
					try $makerow
					try {
						uplevel 1 $script
					} on break {} {
						set broken	1
						break
					} on continue {} {
					} on return {r o} {
						set broken	1
						dict incr o -level 1
						dict set o -code return
						set rethrow	[list -options $o $r]
						break
					} on error {r o} {
						set broken	1
						dict incr o -level 1
						set rethrow	[list -options $o $r]
						break
					}
				}

				switch -exact -- $outcome {
					CommandComplete -
					EmptyQueryResponse {
						break
					}
					PortalSuspended {}
					default {
						error "Unexpected outcome from rowbuffer nextbatch: \"$outcome\""
					}
				}
			}

			if {[info exists rethrow]} {
				return {*}$rethrow
			}
		} finally {
			if {[info exists rowbuffer] && [llength [info commands $rowbuffer]] > 0} {
				$rowbuffer destroy
			}
			if {![$con ready_for_query] && ![$con sync_outstanding]} {
				$con Sync
			}
			if {[$con sync_outstanding]} {
				$con skip_to_sync
			}
		}
	}

	#>>>
	forward resultSetCreate ::tdbc::pgwire::resultset create
	method con {} {set con}
	method build_params {} { set build_params }
	method c_types {} { set c_types }
	method rformats {} { set rformats }
	method stmt_name {} { set stmt_name }
}

#>>>
oo::class create ::tdbc::pgwire::resultset { #<<<
	superclass ::tdbc::resultset

	variable {*}{
		con
		datarows
		open
		columns
		rformats
		tcl_encoding
		c_types
		rowcount
		stmt_name
	}

	constructor {stmt args} { #<<<
		switch -exact -- [llength $args] {
			0 {}
			1 {set param_values	[lindex $args 0]}
			default {
				error "Too many arguments, must be stmt ?param_values?"
			}
		}

		#::pgwire::log notice "Constructing resultset, stmt: ($stmt), args: ($args)"
		if {"::tcl::mathop" ni [namespace path]} {
			namespace path [list {*}[namespace path] {*}{
				::tcl::mathop
			}]
		}
		if {[self next] ne ""} next

		set max_rows_per_batch	$::pgwire::default_batchsize
		set rowcount			0
		set datarows			{}
		set con					[$stmt con]
		set c_types				[$stmt c_types]
		set rformats			[$stmt rformats]
		set stmt_name			[$stmt stmt_name]
		set columns				[dict keys $c_types]
		set tcl_encoding		[$con tcl_encoding]

		if {!$::pgwire::accelerators} {
			# No accelerator support, rewrite our nextdict and nextlist methods to use the statement-specific
			# Tcl row builder script compiled by the prepare statement step
			foreach format {dict list} {
				lassign [info class definition [self class] next$format] method_args method_body
				oo::objdefine [self] method next$format $method_args [regsub {\# makerow_start.*\# makerow_end} $method_body [$con tcl_makerow $format $c_types]]
				#::pgwire::log notice "rewrote [self]::next$format body: [lindex [info object definition [self] next$format] 1]"
			}
			# Transcribe the variables defined to be visible in methods from the class to this instance or the
			# rewritten method won't see them
			oo::objdefine [self] variable {*}[info class variables [self class]]
		}

		try [$stmt build_params] on ok parameters {}
		coroutine [namespace current]::portal $con rowbuffer_coro $stmt_name [self] $rformats $parameters $max_rows_per_batch

		set open	1
	}

	#>>>
	destructor { #<<<
		#::pgwire::log notice "resultset [self] destructor"
		if {[llength [info commands [namespace current]::portal]] != 0} {
			#::pgwire::log notice "resultself [self] destructor, portal exists, destroying"
			portal destroy
		}
		if {[info exists con] && [info object isa object $con]} {
			#::pgwire::log notice "resultself [self] destructor, closing portal [self]"
			$con close_portal [self]
		}
		if {[self next] ne ""} next
	}

	#>>>
	method _read_next_batch {} { #<<<
		#::pgwire::log notice "_read_next_batch"
		lassign [portal nextbatch] outcome details batch_datarows
		lappend datarows	{*}$batch_datarows
		switch -exact -- $outcome {
			PortalSuspended	{incr rowcount $details}
			CommandComplete {
				#::pgwire::log notice "Got CommandComplete: \"$details\""
				switch -exact -- [lindex $details 0] {
					SAVEPOINT -
					RELEASE {}
					default {
						incr rowcount	[lindex $details end]
					}
				}
				set open		0
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

			if {[llength $datarows] == 0} {
				#::pgwire::log notice "nextdict out of data, open: $open"
				if {$open} {
					my _read_next_batch
				}
				# Re-check $open here again, _read_next_batch may have changed it
				if {!$open && [llength $datarows] == 0} {
					#::pgwire::log notice "portal closed, returning 0"
					return 0
				}
			}

			set datarows	[lassign $datarows[unset datarows] data]
			#::pgwire::log notice "Popped data: [string length $data], [llength $datarows] rows remain"

			# makerow_start
			set row	[::pgwire::c_makerow $data $c_types $tcl_encoding %format%s]
			# makerow_end
			return 1
		}]
	}

	#>>>
}

#>>>

# vim: foldmethod=marker foldmarker=<<<,>>> ts=4 shiftwidth=4
