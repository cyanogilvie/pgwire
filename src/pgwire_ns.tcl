package require pgwire		;# Temp: for ::pgwire::c_makerow, ::pgwire::log
package require Thread

namespace eval ::pgwire_ns {
	namespace path {
		::tcl::mathop
	}
	namespace export create
	namespace ensemble create -prefixes no

	variable state {}

	# Const lookups <<<
	variable msgtypes {
		frontend {
			CopyData		d
			CopyDone		c
			Bind			B
			Close			C
			CopyFail		f
			Describe		D
			Execute			E
			Flush			H
			FunctionCall	F
			Parse			P
			PasswordMessage	p
			Query			Q
			Sync			S
			Terminate		X
		}
		backend {
			R	Authentication*
			R0	AuthenticationOk
			R2	AuthenticationKerberosV5
			R3	AuthenticationCleartextPassword
			R5	AuthenticationMD5Password
			R6	AuthenticationSCMCredential
			R7	AuthenticationGSS
			R8	AuthenticationGSSContinue
			R9	AuthenticationSSPI
			K	BackendKeyData
			2	BindComplete
			3	CloseComplete
			C	CommandComplete
			G	CopyInResponse
			H	CopyOutResponse
			D	DataRow
			I	EmptyQueryResponse
			E	ErrorResponse
			V	FunctionCallResponse
			n	NoData
			N	NoticeResponse
			A	NotificationResponse
			t	ParameterDescription
			S	ParameterStatus
			1	ParseComplete
			s	PortalSuspended
			Z	ReadyForQuery
			T	RowDescription
			d	CopyData
			c	CopyDone
		}
	}
	variable encodings_map {
		SQL_ASCII		ascii
		WIN950			ascii
		Windows950		ascii
		BIG5			big5
		EUC_CN			euc-cn
		EUC_JP			euc-jp
		EUC_KR			euc-kr
		LATIN1			iso8859-1
		ISO88591		iso8859-1
		LATIN2			iso8859-2
		ISO88592		iso8859-2
		LATIN3			iso8859-3
		ISO88593		iso8859-3
		LATIN4			iso8859-4
		ISO88594		iso8859-4
		ISO_8859_5		iso8859-5
		ISO_8859_6		iso8859-6
		ISO_8859_7		iso8859-7
		ISO_8859_8		iso8859-8
		LATIN5			iso8859-9
		ISO88599		iso8859-9
		LATIN6			iso8859-10
		ISO885910		iso8859-10
		LATIN7			iso8859-13
		ISO885913		iso8859-13
		LATIN8			iso8859-14
		ISO885914		iso8859-14
		LATIN9			iso8859-15
		ISO885915		iso8859-15
		LATIN10			iso8859-16
		ISO885916		iso8859-16
		KOI8R			koi8-r
		KOI8			koi8-r
		KOI8U			koi8-u
		SJIS			shiftjis
		Mskanji			shiftjis
		ShiftJIS		shiftjis
		WIN932			shiftjis
		Windows932		shiftjis
		UTF8			utf-8
		Unicode			utf-8
	}
	variable errorfields {
		S	Severity
		V	SeverityNL
		C	Code
		M	Message
		D	Detail
		H	Hint
		P	Position
		p	InternalPosition
		q	InternalQuery
		W	Where
		s	Schema
		t	Table
		c	Column
		d	DataType
		n	Constraint
		F	File
		L	Line
		R	Routine
	}
	# Const lookups >>>

	proc create {cmd socket db user password} { #<<<
		variable state

		if {[dict exists $state $socket)]} {
			error "Already connected on $socket"
		}

		dict set state $socket socket			$socket
		dict set state $socket name_seq			0
		dict set state $socket prepared			{}
		dict set state $socket ready_for_query	0
		dict set state $socket age_count		0
		dict set state $socket name_seq			0

		# Initial values <<<
		dict set state $socket transaction_status	I
		dict set state $socket server_params		{}
		dict set state $socket backend_key_data		{}
		dict set state $socket type_oids_rev {
			bytea	    17
			int8		20
			int2		21
			int4		23
			text		25
			float4		700
			float8		701
		}
		dict set state $socket type_oids {
			17		bytea
			20		int8
			21		int2
			23		int4
			25		text
			700		float4
			701		float8
		}
		dict set state $socket tcl_encoding	utf-8
		# Initial values >>>

		chan configure $socket \
			-blocking		1 \
			-translation	binary \
			-encoding		binary \
			-buffering		full

		# Startup <<<
		set payload	[binary format I [expr {
			3 << 16 | 0
		}]]	;# Version 3.0

		::foreach {k v} [list \
			user						$user \
			database					$db \
			client_encoding				UTF8 \
			standard_conforming_strings	on \
		] {
			append payload $k \0 $v \0
		}
		append payload \0

		puts -nonewline $socket [binary format Ia* [expr {4+[string length $payload]}] $payload]
		flush $socket

		on_response $socket {
			AuthenticationOk {} {}

			AuthenticationMD5Password salt {
				PasswordMessage $socket md5[_md5_hex [_md5_hex [encoding convertto [dict get $state $socket tcl_encoding] $password$user]]$salt]
				flush $socket
			}
			AuthenticationCleartextPassword	{} {
				PasswordMessage [encoding convertto [dict get $state $socket tcl_encoding] $password]
				flush $socket
			}

			AuthenticationKerberosV5		args {_error $socket "Authentication method not supported"}
			AuthenticationSCMCredential		args {_error $socket "Authentication method not supported"}
			AuthenticationGSS				args {_error $socket "Authentication method not supported"}
			AuthenticationGSSContinue		args {_error $socket "Authentication method not supported"}
			AuthenticationSSPI				args {_error $socket "Authentication method not supported"}
			ReadyForQuery transaction_status break
		}

		simple_query_dict $socket row {
			select
				oid,
				typname
			from
				pg_type
			where
				oid < 10000 and
				oid is not null and
				typname is not null
		} {
			dict set state $socket type_oids		[dict get $row oid] [dict get $row typname]
			dict set state $socket type_oids_rev	[dict get $row typname] [dict get $row oid]
		}
		# Startup >>>

		uplevel 1 [list proc $cmd {method args} [string map [list \
			%socket% [list $socket] \
		] {
			if {$method in {destroy detach detach_raw}} {
				try {
					::pgwire_ns::$method %socket% {*}$args
				} finally {
					rename [lindex [info level 0] 0] {}
				}
			} else {
				tailcall ::pgwire_ns::$method %socket% {*}$args
			}
		}]]
		uplevel 1 [list trace add command $cmd delete [list apply {{socket args} {
			::pgwire_ns::destroy $socket
		}} $socket]]
	}

	#>>>
	proc detach_raw socket { # Detach, without resetting transaction state or command sync <<<
		variable state
		if {$socket ni [chan names]} {
			error "Not connected"
		}

		#package require Thread
		thread::detach $socket
		try {
			# Strip out the cached variants
			dict set state $socket prepared	[dict map {k v} [dict get $state $socket prepared] {dict merge $v {cached {}}}]
			dict get $state $socket
		} finally {
			#destroy $socket
			dict unset state $socket
		}
	}

	#>>>
	proc detach socket { #<<<
		::pgwire::log notice "detach"
		if {$socket ni [chan names]} {
			error "Not connected"
		}

		# Ensure that the handle we're detaching is ready for a query and not in an open transaction <<<
		if {![dict get $state $socket ready_for_query]} {
			puts -nonewline $socket S\u0\u0\u0\u4
			flush $socket
			while 1 {
				if {[binary scan [read $socket 5] aI msgtype len] != 2} {connection_lost $socket}
				incr len -4	;# len includes itself

				switch -exact -- $msgtype {
					Z { # ReadyForQuery <<<
						set data	[read $socket $len]
						if {[eof $socket]} {connection_lost $socket}
						binary scan $data a transaction_status
						dict set state $socket transaction_status $transaction_status
						dict set state $socket ready_for_query 1
						break
						#>>>
					}

					Z - R - E - S - K - C - D - T - t - 1 - 2 - n - s {
						# Eat these while waiting for the Sync response
						if {$len > 0} {
							read $socket $len
							if {[eof $socket]} {connection_lost $socket}
						}
					}

					default {default_message_handler $socket $msgtype $len}
				}
			}
		}

		if {[dict get $state $socket transaction_status] ne "I"} {
			rollback $socket
		}
		# Ensure that the handle we're detaching is ready for a query and not in an open transaction >>>

		tailcall detach_raw $socket
	}

	#>>>
	proc attach handle { #<<<
		variable state
		::pgwire::log notice "attach"
		#package require Thread
		set socket	[dict get $handle socket]
		dict set state $socket $handle
		thread::attach $socket
	}

	#>>>
	proc destroy socket { #<<<
		variable state
		if {$socket in [chan names]} {
			Terminate $socket
			close $socket
		}
		dict unset state $socket
	}

	#>>>
	# Try to find a suitable md5 command <<<
	if {![catch {package require hash}]} {
		proc _md5_hex bytes {
			package require hash
			binary encode hex [hash::md5 $bytes]
		}
	} elseif {![catch {md5 foo} r]} {
		switch -exact -- [binary encode hex $r] {
			acbd18db4cc2f85cedef654fccc4a4d8 { # md5 command returns binary
				proc _md5_hex bytes {
					binary encode hex [md5 $bytes]
				}
			}
			6163626431386462346363326638356365646566363534666363633461346438 { # md5 command returns hex
				proc _md5_hex bytes {
					md5 $bytes
				}
			}
			724c305932307a432b467a74373256507a4d536b32413d3d { # md5 command returns base64
				proc _md5_hex bytes {
					binary encode hex [binary decode base64 [md5 $bytes]]
				}
			}
			default {
				error "md5 command return format not recognised"
			}
		}
	} else {
		# TODO: provide a pure Tcl implementation?
		error "No md5 command available"
	}
	# Try to find a suitable md5 command >>>
	proc _compile_actions {socket actions} { #<<<
		variable msgtypes

		set compiled_actions	{}
		::foreach {messagename msgargs body} $actions {
			set wbody	""
			if {[llength $msgargs] > 0} {
				set linkvarcmds	{}
				::foreach arg $msgargs {
					lappend linkvarcmds	[list upvar 2 $arg $arg]
				}
				append wbody	[join $linkvarcmds \n] \n
				append wbody	[format {lassign $args %s} $msgargs] \n
				append wbody	[format {catch {uplevel 2 %s} r o} [list $body]] \n
			} else {
				append wbody	[format {catch {uplevel 2 %s} r o} [list $body]] \n
			}
			append wbody {list $r $o} \n
			dict set compiled_actions $messagename [list args $wbody]
		}
		set valid_messagenames	[dict values [dict get $msgtypes backend]]
		::foreach key [dict keys $compiled_actions] {
			if {$key ni $valid_messagenames} {
				error "Error compiling action handlers: \"$key\" is not a valid message type, must be one of: [join $valid_messagenames {, }]"
			}
		}
		dict set compiled_actions ErrorResponse \
			[list fields	[format {%s $fields} [namespace code [list ErrorResponse $socket]]]]
		dict set compiled_actions NoticeResponse \
			[list fields	[format {%s $fields} [namespace code [list NoticeResponse $socket]]]]

	}

	#>>>
	proc ErrorResponse {socket fields} { #<<<
		if {[dict exists $fields SeverityNL]} {
			set severity	[dict get $fields SeverityNL]
		} else {
			set severity	[dict get $fields Severity]
		}
		if {[dict exists $fields Code]} {
			set code		[dict get $fields Code]
		} else {
			set code		unknown
		}
		return -level 2 -code error -errorcode [list PGWIRE ErrorResponse $severity $code $fields] "Postgres error: [dict get $fields Message]"
	}

	#>>>
	proc NoticeResponse {socket fields} { #<<<
		# forward to get notices
		::pgwire::log warning "Postgres NoticeResponse:\n\t[join [lmap {k v} $fields {format {%20s: %s} $k $v}] \n\t]"
	}

	#>>>
	proc NotificationResponse {socket pid channel payload} { #<<<
		# forward this to get notification pushes
	}

	#>>>
	proc _error {socket msg} { #<<<
		destroy $socket
		throw {PGWIRE FATAL} $msg
	}

	#>>>
	proc connection_lost socket { #<<<
		destroy $socket
		throw {PG CONNECTION LOST} "Server closed connection"
	}

	#>>>
	if {[catch {binary scan foo\u0bar C* _str}] == 0 && $_str eq "foo"} {
		proc _get_string {data ofs_var} { # Use TIP586's binary scan c-string support <<<
			upvar 1 $ofs_var i
			if {[binary scan C* $data string] == 0} {
				throw unterminated_string "No null terminator found"
			}
			set i	[expr {$i + [string length $string] + 1}]
			set string
		}

		#>>>
	} else {
		proc _get_string {data ofs_var} { #<<<
			upvar 1 $ofs_var i
			set idx	[string first \u0 $data $i]
			if {$idx == -1} {
				throw unterminated_string "No null terminator found"
			}
			set string	[string range $data $i [expr {$idx - 1}]]
			set i		[expr {$idx + 1}]
			set string
		}

		#>>>
	}
	proc read_one_message socket { #<<<
		variable state
		variable msgtypes
		variable encodings_map
		while 1 {
			if {[binary scan [read $socket 5] aI msgtype len] != 2} {connection_lost $socket}
			#::pgwire::log notice "got msgtype ($msgtype)"
			incr len -4		;# len includes itself
			set data	[read $socket $len]
			if {[string length $data] != $len} {
				if {[eof $socket]} {connection_lost $socket}
			}
			#::pgwire::log notice "read $len bytes of data for ($msgtype)"
			try {
				dict get $msgtypes backend $msgtype
			} trap {TCL LOOKUP DICT} {errmsg options} {
				_error $socket  "Invalid msgtype: \"$msgtype\", [binary encode hex $msgtype], probably a sync issue, abandoning connection"
			} on ok messagename {}

			# Parse message <<<
			#::pgwire::log notice "parsing $messagename"
			set messageparams	{}
			switch -exact -- $messagename {
				Authentication* { #<<<
					binary scan $data I subtype
					set msgtype	R$subtype
					try {
						set messagename	[dict get $msgtypes backend $msgtype]
					} trap {TCL LOOKUP DICT} {errmsg options} {
						set messagename	_invalid_$msgtype
					}
					switch -exact -- $messagename {
						AuthenticationMD5Password { # AuthenticationMD5Password <<<
							binary scan $data @4a4 salt
							lappend messageparams $salt
							#>>>
						}
						AuthenticationOk { # AuthenticationOk <<<
							#>>>
						}
						AuthenticationKerberosV5 { # AuthenticationKerberosV5 <<<
							#>>>
						}
						AuthenticationSCMCredential { # AuthenticationSCMCredential <<<
							#>>>
						}
						AuthenticationGSS { # AuthenticationGSS <<<
							#>>>
						}
						AuthenticationGSSContinue { # AuthenticationGSSContinue <<<
							#>>>
						}
						AuthenticationSSPI { # AuthenticationSSPI <<<
							#>>>
						}

						default { #<<<
							::pgwire::log warning "Parsing of msgtype \"$msgtype\" not implemented yet"
							#>>>
						}
					}
					#>>>
				}
				ErrorResponse { # ErrorResponse <<<
					set i	0
					set fields	{}
					while {$i < [string length $data]} {
						set field_type	[string index $data $i]
						if {$field_type eq "\0"} break
						incr i
						set string		[_get_string $data i]
						lappend fields	[dict get $errorfields $field_type] $string
					}
					lappend messageparams	$fields
					#>>>
				}
				ParameterStatus { # ParameterStatus <<<
					set i	0
					set param	[_get_string $data i]
					set value	[_get_string $data i]
					#?? {::pgwire::log debug "Server ParameterStatus \"$param\" -> \"$value\""}
					dict set server_params $param $value
					if {$param eq "client_encoding"} {
						if {[dict exists $encodings_map $value]} {
							dict set state $socket tcl_encoding	[dict get $encodings_map $value]
						} else {
							::pgwire::log warning "No tcl encoding equivalent defined for \"$value\""
							dict set state $socket tcl_encoding	ascii
						}
					}
					continue
					#>>>
				}
				ReadyForQuery { # ReadyForQuery <<<
					binary scan $data a transaction_status
					dict set state $socket transaction_status $transaction_status
					dict set state $socket ready_for_query	1
					lappend messageparams $transaction_status
					#>>>
				}
				NoticeResponse { # NoticeResponse <<<
					set i	0
					set fields	{}
					while {$i < [string length $data]} {
						set field_type	[string index $data $i]
						if {$field_type eq "\0"} break
						incr i
						set string	[_get_string $data i]
						lappend fields	[dict get $errorfields $field_type] $string
					}
					lappend messageparams $fields
					#>>>
				}
				BackendKeyData { # BackendKeyData <<<
					binary scan $data I backend_key_data
					dict set state $socket backend_key_data $backend_key_data
					#::pgwire::log notice "BackendKeyData: $backend_key_data"
					continue
					#>>>
				}
				CommandComplete { # CommandComplete <<<
					set i	0
					lappend messageparams	[_get_string $data i]
					#>>>
				}
				NotificationResponse { # NotificationResponse <<<
					binary scan $data I pid
					set i 4
					set channel	[_get_string $data i]
					set payload	[_get_string $data i]
					NotificationResponse $socket $pid $channel $payload
					continue
					#>>>
				}
				DataRow { # DataRow <<<
					binary scan $data S column_count
					#?? {::pgwire::log debug "column_count: $column_count"}
					set i	2
					set row	{}
					for {set c 0} {$c < $column_count} {incr c} {
						binary scan $data @${i}I len
						#?? {::pgwire::log debug "Reading column, len: $len"}
						incr i 4
						if {$len == -1} {
							# Column is NULL
							lappend row 1 {}
						} else {
							binary scan $data @${i}a${len} column_data
							lappend row 0 $column_data
							incr i $len
						}
					}
					#?? {::pgwire::log debug "Output row: ($row)"}
					lappend messageparams $row
					#>>>
				}
				RowDescription { # RowDescription <<<
					binary scan $data S fields_per_row
					set i	2
					set column_desc	{}
					for {set c 0} {$c < $fields_per_row} {incr c} {
						set field_name	[_get_string $data i]
						binary scan $data @${i}ISISIS \
								table_oid \
								attrib_num \
								type_oid \
								data_size \
								type_modifier \
								format
						incr i 18
						if {[dict exists $state $socket type_oids $type_oid]} {
							set type_name	[dict get $state $socket type_oids $type_oid]
						} else {
							set type_name	__unknown($type_oid)
						}
						dict set column_desc $field_name [dict create \
								table_oid		$table_oid \
								attrib_num		$attrib_num	\
								type_oid		$type_oid \
								type_name		$type_name \
								data_size		$data_size \
								type_modifier	$type_modifier \
								format			$format \
						]
					}
					lappend messageparams $column_desc
					#>>>
				}
				ParameterDescription { # ParameterDescription <<<
					binary scan $data S count
					binary scan $data @2I${count} parameter_type_oids
					#lappend messageparams [lmap oid $parameter_type_oids {expr {[dict exists $state $socket type_oids $oid] ? [dict get $state $socket type_oids $oid] : $oid}}]

					lappend messageparams [lmap e $parameter_type_oids {dict get $state $socket type_oids $e}]
					#>>>
				}
				ParseComplete { # ParseComplete <<<
					#>>>
				}
				BindComplete { # BindComplete <<<
					#>>>
				}
				CloseComplete { # CloseComplete <<<
					#>>>
				}
				NoData { # NoData <<<
					#>>>
				}
				PortalSuspended { # PortalSuspended <<<
					#>>>
				}

				default { #<<<
					::pgwire::log warning "Parsing of msgtype \"$msgtype\" not implemented yet"
					#>>>
				}
			}
			# Parse message >>>

			#::pgwire::log notice "read_one_message returning [list $messagename $messageparams]"
			return [list $messagename $messageparams]
		}
	}

	#>>>
	proc on_response {socket actions} { #<<<
		variable state
		set compiled_actions	[_compile_actions $socket $actions]
		while 1 {
			set messages	[read_one_message $socket]
			set chainres	{}
			::foreach {messagetype messageparams} $messages {
				#::pgwire::log notice "messagetype: ($messagetype), messageparams: ($messageparams)"
				try {
					lassign [apply [dict get $compiled_actions $messagetype] {*}$messageparams] \
						res options
					return -options $options $res
				} on break {res options} {
					set chainres	[list $res $options]
					break
				} trap {TCL LOOKUP DICT} {errmsg options} {
					::pgwire::log error "Unhandled message: $messagetype:\n$messageparams\n-----------\n[join $compiled_actions \n]\n[dict get $options -errorinfo]"
					set frames	{}
					for {set i [info frame]} {$i > 1} {incr i -1} {
						lappend frames	[info frame $i]
					}
					::pgwire::log error "frames:\n-*- [join $frames "\n-*- "]"
					Terminate $socket
					destroy $socket
					throw {PGWIRE UNSYNCED} "Unexpected message \"$messagetype\" in current context"
				} on error {errmsg options} {
					return -options $options "Action $messagetype handler error: $errmsg"
				}
			}
			if {[llength $chainres] > 0} {
				lassign $chainres res options
				return -options $options $res
			}
		}
	}

	#>>>
	proc sync socket { #<<<
		variable state
		puts -nonewline $socket S\u0\u0\u0\u4
		flush $socket
		while 1 {
			if {[binary scan [read $socket 5] aI msgtype len] != 2} {connection_lost $socket}
			incr len -4
			if {$msgtype eq "Z"} { # ReadyForQuery
				if {[binary scan [read $socket $len] a transaction_status] != 1} {connection_lost $socket}
				dict set state $socket transaction_status $transaction_status
				dict set state $socket ready_for_query 1
				break
			} else {
				default_message_handler $socket $msgtype $len
			}
		}
		set transaction_status
	}

	#>>>
	proc skip_to_sync socket { #<<<
		variable state
		if {$socket in [chan names]} {
			while 1 {
				if {[binary scan [read $socket 5] aI msgtype len] != 2} {connection_lost $socket}
				incr len -4	;# len includes itself

				switch -exact -- $msgtype {
					Z { # ReadyForQuery <<<
						set data	[read $socket $len]
						if {[eof $socket]} {connection_lost $socket}
						binary scan $data a transaction_status
						dict set state $socket transaction_status $transaction_status
						dict set state $socket ready_for_query 1
						break
						#>>>
					}

					Z - R - E - S - K - C - D - T - t - 1 - 2 - n - s {
						# Eat these while waiting for the Sync response
						if {$len > 0} {read $socket $len}
						if {[eof $socket]} {connection_lost $socket}
					}

					default {default_message_handler $socket $msgtype $len}
				}
			}
		}
	}

	#>>>
	proc Terminate socket { #<<<
		#::pgwire::log notice "Sending Terminate"
		puts -nonewline $socket X\u0\u0\u0\u4
	}

	#>>>
	proc PasswordMessage {socket password} { #<<<
		variable state
		set payload	[encoding convertto [dict get $state $socket tcl_encoding] $password]\u0
		puts -nonewline $socket [binary format aIa* p [+ 4 [string length $payload]] $payload]
	}

	#>>>
	proc Query {socket sql} { #<<<
		variable state
		set payload	[encoding convertto [dict get $state $socket tcl_encoding] $sql]\u0
		puts -nonewline $socket [binary format aIa* Q [+ 4 [string length $payload]] $payload]
	}

	#>>>

	proc val {socket str} { # Quote a SQL value <<<
		variable state
		if {
			[dict exists $state $socket server_params standard_conforming_strings] &&
			[dict get $state $socket server_params standard_conforming_strings] eq "on"
		} {
			return '[string map {' ''} $str]'
		} else {
			return '[string map {' '' \\ \\\\} $str]'
		}
	}

	#>>>
	proc name str { # Quote a SQL identifier <<<
		return \"[string map {"\"" "\"\"" "\\" "\\\\"} $str]\"
	}

	#>>>
	proc begintransaction socket { tailcall simple_query_list $socket begin }
	proc rollback socket { #<<<
		variable state
		if {[dict get $state $socket transaction_status] ne "I"} {tailcall simple_query_list $socket rollback}
	}

	#>>>
	proc commit socket { #<<<
		variable state
		if {[dict get $state $socket transaction_status] ne "I"} {tailcall simple_query_list $socket commit}
	}

	#>>>
	proc simple_query_dict {socket rowdict sql on_row} { #<<<
		variable state
		if {![dict get $state $socket ready_for_query]} {
			error "Re-entrant query while another is processing: $sql"
		}
		upvar 1 $rowdict row
		package require tdbc
		set quoted	{}
		::foreach tok [tdbc::tokenize $sql] {
			switch -glob -- $tok {
				::* {
					# Prevent PG casts from looking like params
					append compiled $tok
				}

				:* - $* - @* {
					set name	[string range $tok 1 end]
					if {[regexp {^[0-9]+$} $name]} {
						# Avoid matching $1, $4, etc in the statement (which usually aren't intended
						# to be parameters from us, but refer to the argument of functions).  Not
						# watertight, but would require a SQL-dialect aware parser to do properly.
						append compiled $tok
						continue
					}
					set exists	[uplevel 1 [list info exists $name]]
					if {$exists} {
						set value	[uplevel 1 [list set $name]]
						append quoted	[val $socket $value]
					} else {
						append quoted	NULL
					}
				}

				; {
					append compiled $tok
				}

				default {
					append quoted $tok
				}
			}
		}

		Query $socket $quoted
		flush $socket
		on_response $socket {
			RowDescription columns {set result_columns $columns}
			DataRow column_values {
				set row {}
				::foreach {isnull value} $column_values name [dict keys $result_columns] {
					if {$isnull} {
					} else {
						dict set row $name $value
					}
				}
				uplevel 1 $on_row
			}
			CommandComplete {} {}
			ReadyForQuery transaction_status break
		}
	}

	#>>>
	proc simple_query_list {socket rowlist sql on_row} { #<<<
		variable state
		if {![dict get $state $socket ready_for_query]} {
			error "Re-entrant query while another is processing: $sql"
		}
		upvar 1 $rowlist row
		package require tdbc
		set quoted	{}
		::foreach tok [tdbc::tokenize $sql] {
			switch -glob -- $tok {
				::* {
					# Prevent PG casts from looking like params
					append compiled $tok
				}

				:* - $* - @* {
					set name	[string range $tok 1 end]
					if {[regexp {^[0-9]+$} $name]} {
						# Avoid matching $1, $4, etc in the statement (which usually aren't intended
						# to be parameters from us, but refer to the argument of functions).  Not
						# watertight, but would require a SQL-dialect aware parser to do properly.
						append compiled $tok
						continue
					}
					set exists	[uplevel 1 [list info exists $name]]
					if {$exists} {
						set value	[uplevel 1 [list set $name]]
						append quoted	[val $socket $value]
					} else {
						append quoted	NULL
					}
				}

				; {
					error "Multiple statements are not supported"
				}

				default {
					append quoted $tok
				}
			}
		}

		Query $socket $quoted
		flush $socket
		on_response $socket {
			RowDescription columns {set result_columns $columns}
			DataRow column_values {
				set row	[lmap {isnull value} $column_values {set value}]
				uplevel 1 $on_row
			}
			CommandComplete {} {}
			ReadyForQuery transaction_status break
		}
	}

	#>>>
	proc default_message_handler {socket msgtype len} { #<<<
		variable state
		variable encodings_map
		variable errorfields

		switch -exact -- $msgtype {
			E - N { # ErrorResponse / NoticeResponse <<<
				set data	[read $socket $len]
				if {[eof $socket]} {connection_lost $socket}

				set i	0
				set fields	{}
				while {$i < [string length $data]} {
					set field_type	[string index $data $i]
					if {$field_type eq "\u0"} break
					incr i

					set idx	[string first "\u0" $data $i]
					if {$idx == -1} {
						throw unterminated_string "No null terminator found"
					}
					set string	[string range $data $i [- $idx 1]]
					set i	[+ $idx 1]

					# SeverityNL:	Same as Severity, but not localized
					lappend fields	[dict get $errorfields $field_type] $string
				}

				if {$msgtype eq "E"} {
					ErrorResponse $socket $fields
				} else {
					NoticeResponse $socket $fields
				}
				#>>>
			}
			S { # ParameterStatus <<<
				set data	[read $socket $len]
				if {[eof $socket]} {connection_lost $socket}

				set i	0

				set idx	[string first "\u0" $data $i]
				if {$idx == -1} {
					throw unterminated_string "No null terminator found"
				}
				set param	[string range $data $i [- $idx 1]]
				set i	[+ $idx 1]

				set idx	[string first "\u0" $data $i]
				if {$idx == -1} {
					throw unterminated_string "No null terminator found"
				}
				set value	[string range $data $i [- $idx 1]]

				dict set state $socket server_params $param $value
				if {$param eq "client_encoding"} {
					if {[dict exists $encodings_map $value]} {
						dict set state $socket tcl_encoding	[dict get $encodings_map $value]
					} else {
						::pgwire::log warning "No tcl encoding equivalent defined for \"$value\""
						dict set state $socket tcl_encoding	ascii
					}
				}
				#>>>
			}
			A { # NotificationResponse <<<
				set data	[read $socket $len]
				if {[eof $socket]} {connection_lost $socket}
				binary scan $data I pid
				set i 4

				set idx	[string first "\u0" $data $i]
				if {$idx == -1} {
					throw unterminated_string "No null terminator found"
				}
				set channel	[string range $data $i [- $idx 1]]
				set i	[+ $idx 1]

				set idx	[string first "\u0" $data $i]
				if {$idx == -1} {
					throw unterminated_string "No null terminator found"
				}
				set payload	[string range $data $i [- $idx 1]]

				NotificationResponse $socket $pid $channel $payload
				#>>>
			}
			3 { # CloseComplete <<<
				# Accept these here so that we don't have to wait in a blocking loop for each statement closed
				#>>>
			}
			default { #<<<
				_error $socket "Invalid msgtype: \"$msgtype\", [binary encode hex $msgtype], probably a sync issue, abandoning the connection"
				#>>>
			}
		}
	}

	#>>>
	proc tokenize sql { # Parse the bind bariables from the SQL <<<
		set seq				0
		set params_assigned {}
		set compiled		{}

		::foreach tok [tdbc::tokenize $sql] {
			switch -glob -- $tok {
				::* {
					# Prevent PG casts from looking like params
					append compiled $tok
				}

				:* - $* - @* {
					set name	[string range $tok 1 end]
					if {[regexp {^[0-9]+$} $name]} {
						# Avoid matching $1, $4, etc in the statement (which usually aren't intended
						# to be parameters from us, but refer to the argument of functions).  Not
						# watertight, but would require a SQL-dialect aware parser to do properly.
						append compiled $tok
						continue
					}
					if {![dict exists $params_assigned $name]} {
						set id	[incr seq]
						dict set params_assigned $name id		$id
					} else {
						set id	[dict get $params_assigned $name id]
					}

					append compiled \$$id
				}
				; {
					#error "Multiple statements are not supported"
					# These may be within a function definition or similar.  Build it here and let
					# the backend sort out whether it will accept it
					append compiled $tok
				}
				default {
					append compiled $tok
				}
			}
		}

		list $params_assigned $compiled
	}

	#>>>
	proc extended_query {socket sql variant_setup {max_rows_per_batch 0}} { #<<<
		variable state
		if {![dict get $state $socket ready_for_query]} {
			error "Re-entrant query while another is processing: $sql"
		}
		if {![dict exists $state $socket prepared $sql]} { # Prepare statement <<<
			package require tdbc
			set name_seq	[dict get $state $socket name_seq]
			set stmt_name	[incr name_seq]
			dict set state $socket name_seq $name_seq
			#::pgwire::log notice "No prepared statement, creating one called ($stmt_name)"

			lassign [tokenize $sql] \
				params_assigned \
				compiled

			set age_count	[dict get $state $socket age_count]
			dict set state $socket age_count [incr age_count]
			if {[incr age_count] > 10} {
				if {[dict size [dict get $state $socket prepared]] > 50} {
					log_duration "Cache check" {
					# Age cache <<<
					set expired	0
					# Pre-scan for one-offs to expire <<<
					dict for {sql info} [dict get $state $socket prepared] {
						if {[dict get $info heat] == 0} {
							set closemsg	S[encoding convertto [dict get $state $socket tcl_encoding] [dict get $info stmt_name]]\u0
							puts -nonewline $socket [binary format aIa* C [+ 4 [string length $closemsg]] $closemsg]
							incr expired
							dict unset state $socket prepared $sql
						}
					}
					# Pre-scan for one-offs to expire >>>
					if {[dict size [dict get $state $socket prepared]] > 50} {
						#::pgwire::log notice "Prescan expired $expired entries, [dict size [dict get $state $socket prepared]] remain, aging:\n\t[join [lmap {k v} [dict get $state $socket prepared] {format {%s: %3d} $k [dict get $v heat]}] \n\t]"
						# Age the entries that have hits <<<
						dict for {sql info} [dict get $state $socket prepared] {
							set heat	[expr {[dict get $info heat] >> 1}]
							if {$heat eq 0} {
								set closemsg	S[encoding convertto [dict get $state $socket tcl_encoding] [dict get $info stmt_name]]\u0
								puts -nonewline $socket [binary format aIa* C [+ 4 [string length $closemsg]] $closemsg]
								incr expired
								dict unset state $socket prepared $sql
							} else {
								dict set state $socket prepared $sql heat $heat
							}
						}
						# Age the entries that have hits >>>
					}
					# Age cache >>>
					}
				}
				set age_count	0
			}

			try {
				# Parse, to $stmt_name
				# Describe S $stmt_name
				# Flush
				set parsemsg	[encoding convertto [dict get $state $socket tcl_encoding] $stmt_name]\u0[encoding convertto [dict get $state $socket tcl_encoding] $compiled]\u0\u0\u0
				set describemsg	S[encoding convertto [dict get $state $socket tcl_encoding] $stmt_name]\u0
				dict set state $socket ready_for_query	0
				puts -nonewline $socket [binary format aI P [+ 4 [string length $parsemsg]]]$parsemsg[binary format aI D [expr {4+[string length $describemsg]}]]${describemsg}H\u0\u0\u0\u4

				flush $socket

				# Parse responses <<<
				while 1 {
					if {[binary scan [read $socket 5] aI msgtype len] != 2} {connection_lost $socket}
					incr len -4	;# len includes itself
					if {$msgtype eq 1} break elseif {$msgtype eq 3} {
						# Eat any CloseComplete (3) msgs here from the cache
						# expiries above (to save a round-trip time when
						# expiring statements)
						#incr expired -1
						#::pgwire::log notice "Got CloseComplete, $expired more expected"
					} else {
						default_message_handler $socket $msgtype $len
					}
				}
				#>>>
				# Describe responses <<<
				set field_names		{}
				set param_desc		{}
				while 1 {
					if {[binary scan [read $socket 5] aI msgtype len] != 2} {connection_lost $socket}
					incr len -4	;# len includes itself

					switch -exact -- $msgtype {
						t { # ParameterDescription <<<
							binary scan [read $socket $len] SI* count parameter_type_oids
							if {[eof $socket]} {connection_lost $socket}

							set build_params	{
								set pdesc		{}
								set pformats	{}
							}
							append build_params [list set pcount [llength $parameter_type_oids]] \n

							set param_seq	0
							::foreach oid $parameter_type_oids name [dict keys $params_assigned] {
								set type_name	[dict get $state $socket type_oids $oid]

								set encode_field	[switch -exact -- $type_name {
									bool	{return -level 0 {append pformats \u0\u1; append pdesc [binary format Ic 1 [expr {!!($value)}]]}}
									int1	{return -level 0 {append pformats \u0\u1; append pdesc [binary format Ic 1 $value]}}
									smallint -
									int2	{return -level 0 {append pformats \u0\u1; append pdesc [binary format IS 1 $value]}}
									int4 -
									integer	{return -level 0 {append pformats \u0\u1; append pdesc [binary format II 4 $value]}}
									bigint -
									int8	{return -level 0 {append pformats \u0\u1; append pdesc [binary format IW 8 $value]}}
									bytea	{return -level 0 {append pformats \u0\u1; append pdesc [binary format Ia* [string length $value] $value]}}
									float4	{return -level 0 {append pformats \u0\u1; append pdesc [binary format IR 4 $value]}}
									float8	{return -level 0 {append pformats \u0\u1; append pdesc [binary format IQ 8 $value]}}
									varchar -
									default {
										return -level 0 {
											set bytes	[encoding convertto [dict get $state $socket tcl_encoding] $value]
											set bytelen	[string length $bytes]
											append pformats \u0\u0; append pdesc [binary format Ia$bytelen $bytelen $bytes]
										}
									}
								}]

								append build_params	[string map [list \
								   %v%				[list $name] \
								   %null%			[list [binary format I -1]] \
								   %encode_field%	$encode_field \
								] {
									if {[uplevel 1 {info exists %v%}]} {
										set value	[uplevel 1 {set %v%}]
										%encode_field%
									} else {
										append pdesc	%null%
										append pformats	\u0\u1	;# binary
									}
								}]
								append param_desc	"# \$[incr param_seq]: $name\t$type_name\n"
							}

							append build_params	{
								set parameters	[binary format Sa*Sa* $pcount $pformats $pcount $pdesc]
							} \n
							#>>>
						}
						T { # RowDescription <<<
							set makerow_vars	{}
							set makerow_dict	{}
							set makerow_list	{}
							set colnum	0

							append makerow_vars	{binary scan [read $socket 2] S col_count} \n
							append makerow_dict	{binary scan [read $socket 2] S col_count} \n {set row {}} \n
							append makerow_dict_no_nulls	{binary scan [read $socket 2] S col_count} \n {set row {}} \n
							append makerow_list	{binary scan [read $socket 2] S col_count} \n {set row {}} \n

							set data	[read $socket $len]
							if {[eof $socket]} {connection_lost $socket}

							binary scan $data S fields_per_row
							set i	2
							set c_types	{}
							set rformats	[binary format Su $fields_per_row]
							for {set c 0} {$c < $fields_per_row} {incr c} {
								set idx	[string first \u0 $data $i]
								if {$idx == -1} {
									throw unterminated_string "No null terminator found"
								}
								set field_name	[string range $data $i [- $idx 1]]
								set i	[+ $idx 1]
								incr colnum
								set myname	field_$colnum
								lappend field_names	$field_name field_$colnum

								#binary scan $data @${i}ISISIS \
								#		table_oid \
								#		attrib_num \
								#		type_oid \
								#		data_size \
								#		type_modifier \
								#		format
								#incr i 18
								incr i 6
								binary scan $data @${i}I type_oid
								incr i 12
								set setvar		{}
								if {[dict exists $state $socket type_oids $type_oid]} {
									set type_name	[dict get $state $socket type_oids $type_oid]
									set colfmt		\u0\u1	;# binary
									append setvar [switch -glob -- $type_name {
										bool	{format {set %s [expr {[read $socket $collen] eq "\u1"}]} [list $myname]}
										int1	{format {binary scan [read $socket $collen] c %s} [list $myname]}
										smallint -
										int2	{format {binary scan [read $socket $collen] S %s} [list $myname]}
										int4 -
										integer	{format {binary scan [read $socket $collen] I %s} [list $myname]}
										bigint -
										int8	{format {binary scan [read $socket $collen] W %s} [list $myname]}
										bytea	{format {set %s [read $socket $collen]} [list $myname]}
										float4	{format {binary scan [read $socket $collen] R %s} [list $myname]}
										float8	{format {binary scan [read $socket $collen] Q %s} [list $myname]}
										varchar -
										text	{
											format {set %s [if {$collen == 0} {return -level 0 {}} else {encoding convertfrom %s [read $socket $collen]}]} [list $myname] [list [dict get $state $socket tcl_encoding]]
										}
										default	{
											set colfmt	\u0\u0	;# text
											format {set %s [if {$collen == 0} {return -level 0 {}} else {encoding convertfrom %s [read $socket $collen]}]} [list $myname] [list [dict get $state $socket tcl_encoding]]
										}
									}]

									append rformats	$colfmt
									lappend c_types	$field_name $type_name
								} else {
									append setvar [format {set %s [if {$collen == 0} {return -level 0 {}} else {encoding convertfrom %s [read $socket $collen]}]} [list $myname] [list [dict get $state $socket tcl_encoding]]]
									append rformats	\u0\u0	;# text
									lappend c_types	$field_name text
								}

								if {![regexp "^set [list $myname] \\\[(.*)\\\]$" $setvar - set_res]} {
									set set_res	"$setvar; set [list $myname]"
								}

								set makerow_prefix	"binary scan \[read \$socket 4\] I collen\n"
								append makerow_vars $makerow_prefix "if {\$collen == -1} {unset -nocomplain [list $myname]} else [list $setvar]\n"
								append makerow_dict $makerow_prefix "if {\$collen != -1} {dict set row [list $field_name] \[$set_res\]}\n"
								append makerow_dict_no_nulls $makerow_prefix "if {\$collen != -1} {dict set row [list $field_name] \[$set_res\]} else {dict set row [list $field_name] {}}\n"
								append makerow_list $makerow_prefix "if {\$collen == -1} {lappend row {}} else {lappend row \[$set_res\]}\n"
							}

							break
							#>>>
						}
						n { # NoData <<<
							set rformats		[binary format Su 0]
							set makerow_vars	{}
							set makerow_dict	{}
							set makerow_dict_no_nulls	{}
							set makerow_list	{}
							set c_types			{}
							break
							#>>>
						}
						default {default_message_handler $socket $msgtype $len}
					}
				}
				#>>>
				# Build execute script <<<
				set desc	"# SQL extended-query execute script for:\n# [join [split $compiled \n] "\n# "]\n" 
				append desc "# - Parameters ------------------------\n$param_desc"
				append desc "# - Variant ---------------------------\n# [join [split $variant_setup \n] "\n# "]\n"

				set execute [string map [list \
					%desc%				$desc \
					%build_params%		$build_params \
					%stmt_name%			[list $stmt_name] \
					%field_names%		[list $field_names] \
					%rformats%			[list $rformats] \
					%c_types%			[list $c_types] \
				] {%desc%
					variable state
					set field_names	%field_names%
					set c_types		%c_types%

					# Build params <<<
					%build_params%
					# Build params >>>

					# Execute setup <<<
					%execute_setup%
					# Execute setup >>>

					# Bind statment $stmt_name, default portal
					# Execute $max_rows_per_batch
					# Sync
					dict set state $socket ready_for_query	0
					set rformats	%rformats%
					set bindmsg		\u0[encoding convertto [dict get $state $socket tcl_encoding] %stmt_name%]\u0$parameters$rformats
					puts -nonewline $socket [binary format \
						{ \
							aIa* \
							aIcI \
							a* \
						} \
							B [+ 4 [string length $bindmsg]] $bindmsg \
							E 9 0 $max_rows_per_batch \
							S\u0\u0\u0\u4 \
					]
					flush $socket
					#set rtt_start	[clock microseconds]

					try {
						# Bind responses <<<
						while 1 {
							if {[binary scan [read $socket 5] aI msgtype len] != 2} {connection_lost $socket}
							#if {[info exists rtt_start]} {
							#	::pgwire::log notice "server RTT: [format %.6f [expr {([clock microseconds] - $rtt_start)/1e6}]]"
							#}
							incr len -4	;# len includes itself
							if {$msgtype eq 2} break else {default_message_handler $socket $msgtype $len}
						}
						#>>>
						# Execute responses <<<
						while 1 {
							if {[binary scan [read $socket 5] aI msgtype len] != 2} {connection_lost $socket}
							incr len -4	;# len includes itself
							if {$msgtype eq "D"} { # DataRow <<<
								%on_datarow%
								#>>>
							} else {
								switch -exact -- $msgtype {
									C { # CommandComplete <<<
										set data	[read $socket $len]
										if {[eof $socket]} {connection_lost $socket}
										set idx	[string first "\u0" $data]
										if {$idx == -1} {
											throw unterminated_string "No null terminator found"
										}
										set message	[string range $data 0 [- $idx 1]]
										# TODO: report this somehow?
										break
										#>>>
									}
									I { # EmptyQueryResponse <<<
										break
										#>>>
									}
									s { # PortalSuspended <<<
										# Execute unnamed portal
										puts -nonewline $socket [binary format aIcI E 9 0 $max_rows_per_batch]
										flush $socket
										#>>>
									}
									G { # CopyInResponse <<<
										read $socket $len
										if {[eof $socket]} {connection_lost $socket}
										# Send CopyFail response
										set message	"not supported\u0"
										puts -nonewline $socket [binary format aIa* f [+ 4 [string length $message]] $msg 0]
										#>>>
									}
									H - d - c { # CopyOutResponse / CopyData / CopyDone <<<
										if {$len > 0} {read $socket $len}
										if {[eof $socket]} {connection_lost $socket}
										#error "CopyOutResponse not supported yet"
										#>>>
									}
									default {default_message_handler $socket $msgtype $len}
								}
							}
						}
						#>>>
						# Sync response <<<
						while 1 {
							if {[binary scan [read $socket 5] aI msgtype len] != 2} {connection_lost $socket}
							incr len -4	;# len includes itself

							switch -exact -- $msgtype {
								Z { # ReadyForQuery <<<
									if {![binary scan [read $socket $len] a transaction_status] == 1} {connection_lost $socket}
									dict set state $socket transaction_status $transaction_status
									dict set state $socket ready_for_query	1
									break
									#>>>
								}
								default {default_message_handler $socket $msgtype $len}
							}
						}
						#>>>
					} on error {errmsg options} {
						set rethrow	[list -options $options $errmsg]
						::pgwire::log error "Error during execute: $options"
						skip_to_sync $socket
					}

					if {[info exists rethrow]} {
						return {*}$rethrow
					}
					set acc
				}]
				# Build execute script >>>
				set cached		{}
				#::pgwire::log notice "execute script:\n$execute\nmakerow_vars: $makerow_vars\nmakerow_dict: $makerow_dict\nmakerow_list: $makerow_list"
				# Results:
				#	- $stmt_name: the name of the prepared statement (as known to the server)
				#	- $field_names:	the result column names and the local variables they are unpacked into
				#	- $execute: script to run to execute the statement
				#	- $makerow_vars
				#	- $makerow_dict
				#	- $makerow_dict_no_nulls
				#	- $makerow_list
				#	- $cached
				#	- $heat - how frequently this prepared statement has been used recently, relative to others
				dict set state $socket prepared $sql [dict create \
					stmt_name			$stmt_name \
					field_names			$field_names \
					execute				$execute \
					makerow_vars		$makerow_vars \
					makerow_dict		$makerow_dict \
					makerow_dict_no_nulls		$makerow_dict_no_nulls \
					makerow_list		$makerow_list \
					cached				$cached \
					heat				0 \
				]
				#::pgwire::log notice "Finished preparing statement, execute:\n$execute"
			} on error {errmsg options} { #<<<
				#::pgwire::log error "Error preparing statement,\n$sql\n-- compiled ----------------\n$compiled\ syncing: [dict get $options -errorinfo]"
				if {[info exists socket]} {
					puts -nonewline $socket S\u0\u0\u0\u4
					flush $socket
					skip_to_sync $socket
				}
				return -options $options $errmsg
			}
			#>>>
			#>>>
		} else { # Retrieve prepared statement <<<
			set stmt_info	[dict get $state $socket prepared $sql]
			dict with stmt_info {}
			if {$heat < 128} {
				incr heat
				dict set state $socket prepared $sql heat $heat
			}
			# Sets:
			#	- $stmt_name: the name of the prepared statement (as known to the server)
			#	- $field_names:	the result column names and the local variables they are unpacked into
			#	- $execute: script to run to execute the statement
			#	- $makerow_vars
			#	- $makerow_dict
			#	- $makerow_list
			#	- $cached
			#	- $heat - how frequently this prepared statement has been used recently, relative to others
		}
		#>>>

		try {
			try {
				dict get $cached $variant_setup
			} on ok e {
			} trap {TCL LOOKUP DICT} {} {
				try $variant_setup
				set e	[list {socket max_rows_per_batch} [string map [list \
					%on_datarow%	"$makerow\nif {\[eof \$socket\]} {connection_lost $socket}\n$on_row" \
					%execute_setup%	$execute_setup \
				] $execute] [namespace current]]
				dict set state $socket prepared $sql cached $variant_setup $e
				#writefile /tmp/e [tcl::unsupported::disassemble lambda $e]
				#writefile /tmp/e.tcl $e
			}
			uplevel 1 [list apply $e $socket $max_rows_per_batch]
		} trap {PGWIRE ErrorResponse} {errmsg options} {
			return -code error -errorcode [dict get $options -errorcode] $errmsg
		} on error {errmsg options} {
			#::pgwire::log error "Error in execution phase of extended query: [dict get $options -errorinfo]"
			return -options $options $errmsg
		}
	}

	#>>>
	proc allrows {socket args} { #<<<
		package require tdbc
		variable ::tdbc::generalError

		set args	[::tdbc::ParseConvenienceArgs $args[set args {}] opts]
		switch -exact -- [llength $args] {
			1 {set sqlcode [lindex $args 0]}
			2 {lassign $args sqlcode dict}
			default {
				return -code error -errorcode [concat $generalError wrongNumArgs] \
						"wrong # args: should be [lrange [info level 0] 0 1]\
						 ?-option value?... ?--? sqlcode ?dictionary?"
			}
		}

		switch -exact -- [dict get $opts -as] {
			lists {
				if {[dict exists $opts -columnsvariable]} {
					tailcall extended_query $socket $sqlcode [string map [list \
						%columnsvariable%	[list [dict get $opts -columnsvariable]] \
					] {
						set execute_setup	{
							upvar 1 %columnsvariable% column_names
							set column_names	[dict keys $field_names]
							set acc	{}
						}
						if {$::pgwire::accelerators} {
							set makerow			{
								set data	[read $socket $len]
								if {[chan eof $socket]} {connection_lost $socket}
								set row	[::pgwire::c_makerow $data $c_types $rformats [dict get $state $socket tcl_encoding] lists]
							}
						} else {
							set makerow			$makerow_list
						}
						set on_row			{lappend acc $row}
					}]
				} else {
					tailcall extended_query $socket $sqlcode {
						#set execute_setup {
						#	set parts			{}
						#	set column_names	{}
						#	::foreach {f d} $field_names {
						#	   lappend column_names $f
						#		append parts "\[if {\[info exists [list $d]\]} {set [list $d]}] "
						#	}
						#	set __onrow "lappend acc \[dict create $parts\]"
						#}
						set execute_setup	{set acc {}}
						if {$::pgwire::accelerators} {
							set makerow			{
								set data	[read $socket $len]
								if {[chan eof $socket]} {connection_lost $socket}
								set row	[::pgwire::c_makerow $data $c_types $rformats [dict get $state $socket tcl_encoding] lists]
							}
						} else {
							set makerow			$makerow_list
						}
						set on_row			{lappend acc $row}
					}
				}
			}

			dicts {
				tailcall extended_query $socket $sqlcode {
					#set execute_setup {
					#	set parts	{}
					#	::foreach {f d} $field_names {
					#		append parts "{*}\[if {\[info exists [list $d]\]} {list [list $f] \[set [list $d]\]}\] "
					#	}
					#	set __onrow "lappend acc \[dict create $parts\]"
					#	set __onrow "lappend acc \$row"
					#	::pgwire::log notice "__onrow:\n$__onrow"
					#}
					set execute_setup	{set acc {}}
					if {$::pgwire::accelerators} {
						set makerow			{
							set data	[read $socket $len]
							if {[chan eof $socket]} {connection_lost $socket}
							set row	[::pgwire::c_makerow $data $c_types $rformats [dict get $state $socket tcl_encoding] dicts]
						}
					} else {
						set makerow			$makerow_dict
					}
					set on_row			{lappend acc $row}
				}
			}

			default {
				error "-as must be dicts or lists"
			}
		}
	}

	#>>>
	proc foreach {socket args} { #<<<
		package require tdbc
		variable ::tdbc::generalError

		set args	[::tdbc::ParseConvenienceArgs $args[set args {}] opts]
		switch -exact -- [llength $args] {
			3 {lassign $args row_varname sqlcode script}
			4 {lassign $args row_varname sqlcode dict script}
			default {
				return -code error -errorcode [concat $generalError wrongNumArgs] \
						"wrong # args: should be [lrange [info level 0] 0 1]\
						 ?-option value?... ?--? varName sqlcode ?dictionary? script"
			}
		}

		switch -exact -- [dict get $opts -as] {
			lists {
				if {[dict exists $opts -columnsvariable]} {
					tailcall extended_query $socket $sqlcode [string map [list \
						%rowvarname%		[list $row_varname] \
						%columnsvariable%	[list [dict get $opts -columnsvariable]] \
						%script%			[list $script] \
					] {
						set execute_setup	{
							upvar 1 %columnsvariable% column_names  %rowvarname% row
							set column_names	[dict keys $field_names]
							set acc	{}
							set broken	0
						}
						if {$::pgwire::accelerators} {
							set makerow			{
								set data	[read $socket $len]
								if {[chan eof $socket]} {connection_lost $socket}
								if {!$broken} {
									set row	[::pgwire::c_makerow $data $c_types $rformats [dict get $state $socket tcl_encoding] lists]
								}
							}
						} else {
							set makerow			"if {\$broken} {read \$socket \$len} else {\n$makerow_list\n}"
						}
						set on_row			{
							if {!$broken} {
								try {
									uplevel 1 %script%
								} on break {} {
									set broken	1
								} on continue {} {
								} on return {r o} {
									#::pgwire::log notice "foreach script caught return r: ($r), o: ($o)"
									set broken	1
									dict incr o -level 1
									dict set o -code return
									set rethrow	[list -options $o $r]
								} on error {r o} {
									#::pgwire::log notice "foreach script caught error r: ($r), o: ($o)"
									set broken	1
									dict incr o -level 1
									set rethrow	[list -options $o $r]
								}
							}
						}
					}]
				} else {
					tailcall extended_query $socket $sqlcode [string map [list \
						%rowvarname%		[list $row_varname] \
						%script%			[list $script] \
					] {
						set execute_setup	{
							upvar 1 %rowvarname% row
							set column_names	[dict keys $field_names]
							set acc	{}
							set broken	0
						}
						if {$::pgwire::accelerators} {
							set makerow			{
								set data	[read $socket $len]
								if {[chan eof $socket]} {connection_lost $socket}
								if {!$broken} {
									set row	[::pgwire::c_makerow $data $c_types $rformats [dict get $state $socket tcl_encoding] lists]
								}
							}
						} else {
							set makerow			"if {\$broken} {read \$socket \$len} else {\n$makerow_list\n}"
						}
						set on_row			{
							if {!$broken} {
								try {
									uplevel 1 %script%
								} on break {} {
									set broken	1
								} on continue {} {
								} on return {r o} {
									#::pgwire::log notice "foreach script caught return r: ($r), o: ($o)"
									set broken	1
									dict incr o -level 1
									dict set o -code return
									set rethrow	[list -options $o $r]
								} on error {r o} {
									#::pgwire::log notice "foreach script caught error r: ($r), o: ($o)"
									set broken	1
									dict incr o -level 1
									set rethrow	[list -options $o $r]
								}
							}
						}
					}]
				}
			}

			dicts {
				tailcall extended_query $socket $sqlcode [string map [list \
					%rowvarname%		[list $row_varname] \
					%script%			[list $script] \
				] {
					set execute_setup	{
						upvar 1 %rowvarname% row
						set column_names	[dict keys $field_names]
						set acc	{}
						set broken	0
					}
					if {$::pgwire::accelerators} {
						set makerow			{
							set data	[read $socket $len]
							if {[chan eof $socket]} {connection_lost $socket}
							if {!$broken} {
								set row	[::pgwire::c_makerow $data $c_types $rformats [dict get $state $socket tcl_encoding] dicts]
							}
						}
					} else {
						set makerow			"if {\$broken} {read \$socket \$len} else {\n$makerow_dict\n}"
					}
					set on_row			{
						if {!$broken} {
							try {
								uplevel 1 %script%
							} on break {} {
								set broken	1
							} on continue {} {
							} on return {r o} {
								#::pgwire::log notice "foreach script caught return r: ($r), o: ($o)"
								set broken	1
								dict incr o -level 1
								dict set o -code return
								set rethrow	[list -options $o $r]
							} on error {r o} {
								#::pgwire::log notice "foreach script caught error r: ($r), o: ($o)"
								set broken	1
								dict incr o -level 1
								set rethrow	[list -options $o $r]
							}
						}
					}
				}]
			}

			default {
				error "-as must be dicts or lists"
			}
		}
	}

	#>>>
	proc onecolumn {socket sql} { #<<<
		tailcall extended_query $socket $sql {
			set execute_setup	{set broken 0; set acc {}}
			if {0 && $::pgwire::accelerators} {
				set makerow			{
					set data	[read $socket $len]
					if {[chan eof $socket]} {connection_lost $socket}
					if {!$broken} {
						set row	[::pgwire::c_makerow $data $c_types $rformats [dict get $state $socket tcl_encoding] lists]
					}
				}
			} else {
				set makerow			"if {\$broken} {read \$socket \$len} else {\n$makerow_list\n}"
			}
			set on_row			{
				if {!$broken} {
					set acc		[lindex $row 0]
					set broken	1
				}
			}
		}
	}

	#>>>

	proc transaction_status socket { variable state; dict get $state $socket transaction_status }
}

# vim: foldmethod=marker foldmarker=<<<,>>> ts=4 shiftwidth=4
