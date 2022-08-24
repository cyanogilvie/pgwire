package require tcltest
namespace import ::tcltest::*
#namespace eval ::pgwire {variable block_accelerators 1}

::tcltest::loadTestedCommands
package require pgwire

#puts "db: [exec ping -c 1 db]"
package require unix_sockets

#set connect {
#	unix_sockets::connect [file join $::env(PGHOST) .s.PGSQL.5432]
#}
set connect {
	socket db 5432
}

set std_setup {
	set dbchan	[try $connect]
	pgwire create pg $dbchan pagila postgres insecure
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

# vim: ft=tcl foldmethod=marker foldmarker=<<<,>>> ts=4 shiftwidth=4
