#!/usr/bin/env tclsh

proc withfile {handle fn mode script} { #<<<
	upvar 1 $handle h
	set h	[open $fn $mode]
	try {
		uplevel 1 $script
	} finally {
		if {[info exists h] && $h in [chan names]} {
			close $h
		}
	}
}

#>>>
proc readfile fn { #<<<
	withfile h $fn r {read $h}
}

#>>>
proc foreachline {linevar fn script} { #<<<
	upvar 1 $linevar line
	withfile h $fn r {
		while 1 {
			set line	[gets $h]
			if {[eof $h] && $line eq ""} break
			uplevel 1 $script
		}
	}
}

#>>>

namespace eval ::$argv0 {
	namespace export *
	namespace ensemble create -prefixes no

	proc show fn { #<<<
		foreachline line $fn {
			lassign $line dir start end base64bytes
			set bytes	[binary decode base64 $base64bytes]
			if {![info exists datum]} {
				set datum	$start
			}
			set relstart	[expr {$start - $datum}]
			#set relend		[expr {$end - $datum}]
			set duration	[expr {$end - $start}]
			if {[info exists lastend]} {
				set gap		[expr {$start - $lastend}]
			}
			set lastend		$end

			if {[info exists gap]} {
				puts [format {gap: %d usec} $gap]
			}
			puts [format {+%6d %5d:} $relstart $duration]
		}
	}

	#>>>
}

try {
	::$argv0 {*}$argv
} trap {TCL LOOKUP SUBCOMMAND} {errmsg options} - trap {TCL WRONGARGS} {errmsg options} {
	puts stderr $errmsg
	exit 1
}
