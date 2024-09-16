package require tcltest
namespace import ::tcltest::*
#namespace eval ::pgwire {variable block_accelerators 1}

::tcltest::loadTestedCommands
package require pgwire

#puts "db: [exec ping -c 1 db]"
package require unix_sockets

proc _ms {desc script} {
	global _ts_stack
	if {[info exists _ts_stack]} {
		lappend _ts_stack	{}
	} else {
		set _ts_stack {{}}
	}
	set a			[clock microseconds]
	set code		[catch {uplevel 1 $script} r o]
	set end			[clock microseconds]
	set range		[expr {$end - $a}]
	set elapsed		[expr {$range/1e6}]
	set stamps		[lindex $_ts_stack end]
	set _ts_stack	[lrange $_ts_stack 0 end-1]
	if {[llength $_ts_stack] == 0} {unset _ts_stack}
	set msg	"_ms($desc): [format %.3f $elapsed] ms"
	if {[llength $stamps]} {
		append msg	", stamps:\n[join [lmap e $stamps {
			lassign $e usec tsdesc
			set f	[expr {($usec-$a)*100.0/$range}]
			format {  %5.1f %%: %s} $f $tsdesc 
		}] \n]"
	}
	puts $msg
	if {$code == 1} {return -code $code -options $o $r}
	return -code $code $r
}

proc _ts {desc {res {}}} {
	global _ts_stack
	lset _ts_stack end end+1 [list [clock microseconds] $desc]
	set res
}

#set connect {
#	unix_sockets::connect [file join $::env(PGHOST) .s.PGSQL.5432]
#}
set connect {
	socket db 5432
}
set std_setup {
	set _std_setup_before	[clock microseconds]
	try {
		set dbchan	[try $connect]
		pgwire create pg $dbchan pagila postgres insecure
	} finally {
		#puts stderr "std_setup time: [format %.3f [expr {([clock microseconds]-$_std_setup_before)/1e6}]] ms"
		unset -nocomplain _std_setup_before
	}
}

if {[info object isa object pgwire_tap]} {
	pgwire_tap destroy
}
oo::class create pgwire_tap { # Record the network protocol reads and writes with timestamps <<<
	superclass ::pgwire

	variable tap_h

	constructor {tap_fn args} { #<<<
		set tap_h	[open $tap_fn w]
		chan configure $tap_h -blocking 0 -buffering full -buffersize 1000000

		proc puts args { #<<<
			if {[llength $args] == 3} {
				set before [clock microseconds]
			}
			::puts {*}$args
			if {[info exists before]} {
				my _tx $before [clock microseconds] [lindex $args 2]
			}
		}
		#>>>
		proc read args { #<<<
			try {
				set before	[clock microseconds]
				::read {*}$args
			} on ok bytes {
				my _rx $before [clock microseconds] $bytes
				set bytes
			}
		}

		#>>>

		next {*}$args
	}

	#>>>
	destructor { #<<<
		if {[self next] ne ""} next
		if {[info exists tap_h]} {
			close $tap_h
		}
	}

	#>>>

	method _rx {start end bytes} { #<<<
		::puts $tap_h	[list rx $start $end [binary encode base64 $bytes]]
	}

	#>>>
	method _tx {start end bytes} { #<<<
		::puts $tap_h	[list tx $start $end [binary encode base64 $bytes]]
	}

	#>>>
}

#>>>

# Hack to make available the name of the currently running test
trace add execution ::tcltest::test enter [list apply {{cmd args} {set ::testname [lindex $cmd 1]}}]
trace add execution ::tcltest::test leave [list apply {{cmd args} {unset -nocomplain ::testname}}]

if {[info exists ::env(TCPDUMP)] && ![info exists ::tcpdump_tracing]} {
	set ::tcpdump_tracing	1

	if {![info exists server_ip] && ![regexp {^(.*?)\s+db} [exec getent hosts db] - server_ip]} {
		puts stderr "Error looking up server ip"
	} else {
		set netinfo	[exec ip route get $server_ip]
		if {![regexp { dev ([^ ]+) } $netinfo - netdev]} {
			puts stderr "Could not parse server netdev from ($netinfo)"
		} else {
			puts stderr "DB server ip: $server_ip, netdev: ($netdev)"
			set ::tcpdumps	{}
			trace add execution ::tcltest::test enter [list apply {{cmd args} {
				global netdev tcpdumps env
				lassign $cmd - testname description testargs

				set constraints	[if {[dict exists $testargs -constraints]} {dict get $testargs -constraints}]
				if {![::tcltest::Skipped $testname $constraints]} {
					set tcpdump_cmd	[list tcpdump -i $netdev -s 65535 -q -n -U --immediate-mode -w $env(TCPDUMP)$testname]
					set pids	[exec {*}$tcpdump_cmd 2> /dev/null &]
					dict set tcpdumps $testname $pids
					after 100
				}
			}}]
			trace add execution ::tcltest::test leave [list apply {{cmd args} {
				global netdev global tcpdumps
				set testname [lindex $cmd 1]
				if {[dict exists $tcpdumps $testname]} {
					exec kill {*}[dict get $tcpdumps $testname]
					after 100
				}
			}}]
		}
	}
}

# vim: ft=tcl foldmethod=marker foldmarker=<<<,>>> ts=4 shiftwidth=4
