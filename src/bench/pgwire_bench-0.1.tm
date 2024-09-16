package require math::statistics

namespace eval ::bench {
	namespace export *

	variable match		*
	variable run		{}
	variable skipped	{}
	variable skip		{}

	# Override this to a new lambda to capture the output
	variable output {
		{lvl msg} {puts $msg}
	}

	namespace path [concat [namespace path] {
		::tcl::mathop
	}]

proc output {lvl msg} { #<<<
	variable output
	tailcall apply $output $lvl $msg
}

#>>>
proc _intersect3 {list1 list2} { #<<<
	set firstonly       {}
	set intersection    {}
	set secondonly      {}

	set list1	[lsort -unique $list1]
	set list2	[lsort -unique $list2]

	foreach item $list1 {
		if {[lsearch -sorted $list2 $item] == -1} {
			lappend firstonly $item
		} else {
			lappend intersection $item
		}
	}

	foreach item $list2 {
		if {[lsearch -sorted $intersection $item] == -1} {
			lappend secondonly $item
		}
	}

	list $firstonly $intersection $secondonly
}

#>>>
proc _readfile fn { #<<<
	set h	[open $fn r]
	try {
		read $h
	} finally {
		close $h
	}
}

#>>>
proc _writefile {fn dat} { #<<<
	set h	[open $fn w]
	try {
		puts -nonewline $h $dat
	} finally {
		close $h
	}
}

#>>>
proc _run_if_set script { #<<<
	if {$script eq ""} return
	set lambda	[list {} $script ::_bench_private]
	uplevel 2 [list apply $lambda]
}

#>>>
proc _pick {mode variant patterns} { #<<<
	set chain	0
	set found	0
	set res [lmap {pat e} $patterns {
		if {$pat eq {}} {set default $e}
		if {!$chain && ![string match $pat $variant]} continue
		if {$e eq "-"} {
			set chain	1
			continue
		}
		set chain	0
		switch -exact -- $mode {
			first - first!	{return $e}
			all - all!		{set found 1; set e}
			default	{error "Invalid _pick mode: ($mode)"}
		}
	}]
	if {!$found && [info exists default]} {
		return [switch -exact -- $mode {
			all - all!		{list $default}
			default			{set default}
		}]
	}
	if {!$found && [string match *! $mode]} {
		error "No pattern matches for \"$variant\""
	}
	set res
}

#>>>
proc _verify_res {variant retcodes expected match_mode got options} { #<<<
	variable current_bench

	if {[dict get $options -code] ni $retcodes} {
		::bench::output error "Error: $got\n[dict get $options -errorinfo]"
		throw [list BENCH BAD_CODE $current_bench $variant $retcodes [dict get $options -code]] \
			"$current_bench/$variant: Expected codes [list $retcodes], got [dict get $options -code]"
	}

	switch -- $match_mode {
		exact  { if {$got eq $expected} return }
		glob   { if {[string match $expected $got]} return }
		regexp { if {[regexp $expected $got]} return }
		default {
			throw [list BENCH BAD_MATCH_MODE $match_mode] \
				"Invalid match mode \"$match_mode\", should be one of exact, glob, regexp"
		}
	}

	throw [list BENCH BAD_RESULT $current_bench $variant $match_mode $expected $got] \
		"$current_bench/$variant: Expected ($match_mode): -----------\n$expected\nGot: ----------\n$got"
}

#>>>
proc _make_stats times { #<<<
	set res	{}

	foreach stat {
		arithmetic_mean min max number_of_data
		sample_stddev sample_var
		population_stddev population_var
	} val [math::statistics::basic-stats $times] {
		dict set res $stat $val
	}
	dict set res median [math::statistics::median $times]
	dict set res harmonic_mean [/ [llength $times] [+ {*}[lmap time $times {
		/ 1.0 $time
	}]]]
	dict set res cv [expr {[dict get $res population_stddev] / [dict get $res arithmetic_mean]}]
}

#>>>
proc bench {name desc args} { #<<<
	variable match
	variable skip
	variable run
	variable skipped
	variable output
	variable current_bench

	# For auto-cleanup
	set beforevars	[lsort [info vars ::_bench_private::*]]
	set beforecmds	[lsort [info commands ::_bench_private::*]]

	# -target_cv		- Run until the coefficient of variation is below this, up to -max_time
	# -max_time 		- Maximum number of seconds to keep running while the cv is converging
	# -min_time			- Keep accumulating samples for at least this many seconds
	# -batch			- The number of samples to take in a tight loop and average to count as a single sample.  "auto" guesses a reasonable value to make a batch take at least 1000 usec.
	# -window			- Consider at most the previous -window measurements for target_cv and the results
	array set opts {
		-setup			{}
		-compare		{}
		-cleanup		{}
		-batch			auto
		-match			exact
		-returnCodes	{ok return}
		-target_cv		{0.0015}
		-min_time		0.0
		-max_time		1.0
		-min_it			30
		-window			30
		-overhead		{}
		-deps			{}
		-combinations	{}
		-transform		{}
	}
	array set opts $args
	set badargs [lindex [_intersect3 [array names opts] {
		-setup -compare -cleanup -batch -match -result -results -returnCodes -target_cv -min_time -max_time -min_it -window -overhead -deps -combinations -transform
	}] 0]

	if {[llength $badargs] > 0} {
		error "Unrecognised arguments: [join $badargs {, }]"
	}

	if {![string match $match $name] || [string match $skip $name]} {
		lappend skipped $name
		return
	}

	set normalized_codes	[lmap e $opts(-returnCodes) {
		switch -- $e {
			ok			{list 0}
			error		{list 1}
			return		{list 2}
			break		{list 3}
			continue	{list 4}
			default		{set e}
		}
	}]

	set make_blambda {script {
		list c [format {incr c; while {[incr c -1]} {apply %s}} [list [list {} $script ::_bench_private]]]
	}}

	set variant_stats {}

	set overheads	{}

	set current_bench $name
	_run_if_set $opts(-setup)
	try {
		# Expand the variant out to the product of the -combinations <<<
		set expanded_compare	{}
		dict for {variant script} $opts(-compare) {
			set substs	{}
			foreach combsets [_pick all $variant $opts(-combinations)] {
				#apply $output notice "combsets: ($combsets)"
				foreach {pat values} $combsets {
					if {[string first $pat $variant] == -1} continue
					dict lappend substs $pat {*}$values
				}
			}
			#apply $output notice "substs: ($substs) from combinations ($opts(-combinations))"
			set all_c	{{}}
			dict for {pat values} $substs {
				set new_all_c	{}
				foreach c $all_c {
					foreach v $values {
						lappend new_all_c [list {*}$c $pat $v]
					}
				}
				set all_c	$new_all_c
			}

			#apply $output notice "all_c: ($all_c)"
			#apply $output notice "Expanded variant $variant:"
			foreach map $all_c {
				set cscript		$script
				set name_map	{}
				unset -nocomplain script_map
				foreach {pat vals} $map {
					lappend name_map	$pat [lindex $vals 0]
					if {[llength $vals] >= 2} {
						#apply $output notice "${pat}([lindex $vals 0]) defines a script_map: ([lindex $vals 1])"
						lappend script_map	{*}[lindex $vals 1]
					}
				}
				if {[info exists script_map]} {
					set macros	{}
					set script_map	[dict map {pat rep} $script_map {
						if {[regexp {^%(.*?)\((.*)\)%$} $pat - mname arglist]} {
							#                regexp                  lambda
							lappend macros   %${mname}\\((.*?)\\)%   [list $arglist $rep]
							continue
						}
						set rep
					}]
					set cscript	[string map $script_map $cscript]
					if {[llength $macros]} {
						foreach {regexp lambda} $macros {
							set mscript	{}
							set from	0
							foreach {m argvalsidx} [regexp -all -inline -indices $regexp $cscript] {
								set pref	[string range $cscript $from [lindex $m 0]-1]
								set from	[expr {[lindex $m 1]+1}]
								set argvals	[string range $cscript {*}$argvalsidx]
								append mscript $pref [apply $lambda {*}$argvals]
							}
							append mscript	[string range $cscript $from end]
							set cscript	$mscript
						}
					}
					#apply $output notice "    script_map($script_map):\n$cscript"
				}

				dict set expanded_compare [string map $name_map $variant] [string map $name_map $cscript]
				#apply $output notice "    using map ($map) to: ([string map $name_map $variant])"
			}
		}
		#>>>

		dict for {variant script} $expanded_compare {
			set variant_start	[clock microseconds]
			set overhead_script	[join [_pick all $variant $opts(-overhead)] \n]
			foreach transform	[_pick all $variant $opts(-transform)] {
				if {[lindex $transform 2] eq ""} {
					lset transform 2 ::_bench_private
				}
				set script			[apply $transform $script]
				set overhead_script	[apply $transform $overhead_script]
			}
			#apply $output notice "\nVariant: $variant:\n$script\noverhead_script:\n$overhead_script"

			set lambda			[list {} $script ::_bench_private]
			set single_empty	{apply {{} {} ::_bench_private}}
			set single_lambda	[list apply $lambda]
			set blambda			[apply $make_blambda $script]

			try {
				namespace eval ::_bench_private [join [_pick all $variant $opts(-deps)] \n]
			} on error {errmsg options} {
				apply $output notice "Skipping variant $current_bench/$variant: $errmsg"
				#apply $output notice "([join [_pick all $variant $opts(-deps)] \n]): [dict get $options -errorinfo]"
				continue
			}
			# Verify the first result against -result (if given), and estimate an appropriate batchsize to target a batch time of 1 ms to reduce quantization noise <<<
			if 1 $single_empty	;# throw the first away
			catch $single_lambda r o
			unset -nocomplain expected
			if {[info exists opts(-results)]} {
				set expected	[_pick first! $variant $opts(-results)]
			} elseif {[info exists opts(-result)]} {
				set expected	$opts(-result)
			}
			if {[info exists expected]} {
				#apply $output debug "Verifying expected output for $current_bench/$variant"
				_verify_res $variant $normalized_codes $expected $opts(-match) $r $o
			}
			#>>>

			# Try to guess a batch number that means each batch runs for about $target_usec <<<
			if {$opts(-batch) eq "auto"} {
				set target_usec	10000.0
				set target_ms	[expr {round($target_usec / 1000.0)}]
				set target_bit	10
				set target_bms	[expr {round($target_ms*$target_bit)}]
				set tres		[timerate $single_lambda $target_ms]
				set batch		[lindex $tres 2]
				#apply $output notice "Initial guess: $batch: $tres, target_bms: $target_bms"
				set runbatch	[list apply $blambda $batch]
				if 0 {
				apply $output notice "first batch run: [timerate $runbatch 1 1]"
				set tres		[timerate $runbatch $target_bms]
				set it_usec		[lindex $tres 0]
				apply $output notice "wanted: $target_usec, got: $it_usec (with batch size of ($batch): $tres"
				set accuracy	[expr {1.0 - abs($it_usec - $target_usec) / $target_usec}]
				if {0 && $accuracy < 0.9} {
					set bit			[lindex $tres 2]
					set batch		[expr {max(3, round($target_usec * 1.0/$it_usec * $batch))}]
					apply $output notice "accuracy: $accuracy, refined guess: $batch: $tres"
					set runbatch	[list apply $blambda $batch]
					if 1 $runbatch
				}
				apply $output notice "tuned for $target_usec / it: [timerate $runbatch $target_bms]"
				}
			} else {
				set runbatch	[list apply $blambda $batch]
			}

			#apply $output notice "Picked batchsize: $batch, runbatch:\n$runbatch"
			#>>>

			# Measure the instrumentation overhead to compensate for it <<<
			#apply $output debug "Measuring overhead for $current_bench/$variant"
			if {![dict exists $overheads $overhead_script]} {
				# Only run each unique overhead script once (so the related variants are adjusted consistently)
				set blambda_overhead	[list apply [apply $make_blambda $overhead_script] $batch]
				dict set overheads $overhead_script	[lindex [timerate $blambda_overhead 100 1000000] 0]
				#apply $output notice "overhead: [dict get $overheads $overhead_script] for\n$blambda_overhead with batch: ($batch)"
			}
			set overhead			[dict get $overheads $overhead_script]

			set cv {data { # Calculate the coefficient of variation of $data <<<
				lassign [::math::statistics::basic-stats $data] \
					arithmetic_mean min max number_of_data sample_stddev sample_var population_stddev population_var

				expr {
					$population_stddev / double($arithmetic_mean)
				}
			}}
			#>>>

			set begin	[clock microseconds]	;# Don't count the first run time or the overhead measurement into the total elapsed time
			set it		0
			set times	{}
			set means	{}
			set cvmeans	{}
			set cvtimes	{}
			set elapsed	0
			#puts stderr "bscript $variant: $bscript"
			# Run at least:
			# - -min_it times
			# - for half a second
			# - until the coefficient of variability of the means has fallen below -target_cv, or a max of -max_time seconds
			#apply $output notice "$name/$variant timing runbatch:\n$runbatch"
			while {
				[llength $times] < $opts(-min_it) ||
				$elapsed < $opts(-min_time) ||
				($elapsed < $opts(-max_time) && $cvmeans > $opts(-target_cv))
			} {
				#set before	[clock microseconds]
				set batchtime	[lindex [set tres [timerate -overhead $overhead $runbatch 100]] 0]
				if {$batchtime == 0} {
					# The calibration can cause this run to return below the measured overhead and be clamped to 0
					#apply $output notice "Overhead compensation clamping: [expr {([clock microseconds]-$before)}] usec clamped to 0 with -overhead $overhead, tres: $tres"
					set batchtime	[expr {1e-16}]
				}
				lappend times [expr {$batchtime / double($batch)}]
				#apply $output notice "Recorded time [lindex $times end], from \$before: [expr {[clock microseconds]-$before}], batch: $batch, tres: $tres"
				set elapsed		[expr {([clock microseconds] - $begin)/1e6}]
				set cvtimes		[lrange $times end-[+ 1 $opts(-window)] end]	;# Consider the last $opts(-window) data in estimating the variation
				lappend means	[expr {[+ {*}$cvtimes]/[llength $cvtimes]}]
				set _cv			[apply $cv $cvtimes]
				set cvmeans		[apply $cv [lrange $means end-[+ 1 $opts(-window)] end]]
				#apply $output notice "Got time for $variant batch($batch), batchtime $batchtime usec: [format %.4f [lindex $times end]], elapsed: [format %.3f $elapsed] sec[if {[info exists cvmeans]} {format {, cvmeans: %.3f} $cvmeans}][if {[info exists _cv]} {format {, cv: %.3f} $_cv}], mean: [format %.5f [lindex $means end]]"
			}

			dict set variant_stats $variant [_make_stats $cvtimes]
			dict set variant_stats $variant cvmeans		$cvmeans
			dict set variant_stats $variant cv			[apply $cv $cvtimes]
			dict set variant_stats $variant runtime		$elapsed
			dict set variant_stats $variant it			[llength $cvtimes]
			apply $output notice "Measured $name/$variant: [format %.3f [expr {([clock microseconds]-$variant_start)/1e6}]] seconds"
		}

		lappend run $name $desc $variant_stats
	} finally {
		_run_if_set $opts(-cleanup)

		# Auto cleanup <<<
		set aftervars	[lsort [info vars ::_bench_private::*]]
		set aftercmds	[lsort [info commands ::_bench_private::*]]
		#apply $output notice "beforecmds: ($beforecmds)"
		#apply $output notice " aftercmds: ($aftercmds)"
		set cleanvars	{}
		while {[llength $aftervars]} {
			if {[llength $beforevars] == 0} {
				lappend cleanvars	{*}$aftervars
				break
			}
			switch [string compare [lindex $beforevars 0] [lindex $aftervars 0]] {
				-1	{set beforevars	[lrange $beforevars 1 end]}
				0	{set aftervars	[lrange $aftervars 1 end]; set beforevars [lrange $beforevars 1 end]}
				1	{set aftervars	[lassign $aftervars newvar]; lappend cleanvars $newvar}
			}
		}
		#apply $output notice "Autocleaning vars: $cleanvars"
		unset -nocomplain {*}$cleanvars

		set cleancmds	{}
		while {[llength $aftercmds]} {
			if {[llength $beforecmds] == 0} {
				lappend cleancmds	{*}$aftercmds
				break
			}
			switch [string compare [lindex $beforecmds 0] [lindex $aftercmds 0]] {
				-1	{set beforecmds	[lrange $beforecmds 1 end]}
				0	{set aftercmds	[lrange $aftercmds 1 end]; set beforecmds [lrange $beforecmds 1 end]}
				1	{set aftercmds	[lassign $aftercmds newcmd]; lappend cleancmds $newcmd}
			}
		}
		foreach cmd $cleancmds {
			#apply $output notice "Autocleaning cmd ($cmd)"
			rename $cmd {}
		}
		#>>>

		unset current_bench
	}
}; namespace export bench

#>>>
namespace eval display_bench {
	namespace export *
	namespace ensemble create
	namespace path [concat [namespace path] {
		::tcl::mathop
	}]

	proc _heading {txt {char -}} { #<<<
		format {%s %s %s} \
			[string repeat $char 2] \
			$txt \
			[string repeat $char [- 80 5 [string length $txt]]]
	}

	#>>>
	proc short {name desc variant_stats relative {pick median}} { #<<<
		variable output

		::bench::output notice [_heading [format {%s: "%s"} $name $desc]]

		# Gather the union of all the variant names from this and past runs
		set variants	[dict keys $variant_stats]
		foreach {label past_run} $relative {
			foreach past_variant [dict keys $past_run] {
				if {$past_variant ni $variants} {
					lappend variants $past_variant
				}
			}
		}

		# Assemble the tabular data
		lappend rows	[list "" "This run" {*}[dict keys $relative]]
		lappend rows	{*}[lmap variant $variants {
			unset -nocomplain baseline
			set past_stats	[dict values $relative]
			list $variant {*}[lmap stats [list $variant_stats {*}$past_stats] {
				if {![dict exists $stats $variant]} {
					#string cat --
					set _ --
				} else {
					set val		[dict get $stats $variant $pick]
					if {![info exists baseline]} {
						set baseline	$val
						format {%.3f%s} $val [expr {
							[dict exists $stats $variant cv] ? [format { cv:%.1f%%} [expr {100*[dict get $stats $variant cv]}]] : ""
						}]
					} elseif {$baseline == 0} {
						format x%s inf
					} else {
						format {x%.3f%s} [/ $val $baseline] [expr {
							[dict exists $stats $variant cv] ? [format { cv:%.1f%%} [expr {100*[dict get $stats $variant cv]}]] : ""
						}]
					}
				}
			}]
		}]

		# Determine the column widths
		set colsize	{}
		foreach row $rows {
			set cols	[llength $row]
			for {set c 0} {$c < $cols} {incr c} {
				set this_colsize	[string length [lindex $row $c]]
				if {
					![dict exists $colsize $c] ||
					[dict get $colsize $c] < $this_colsize
				} {
					dict set colsize $c $this_colsize
				}
			}
		}

		# Construct the row format description
		set col_fmts	[lmap {c size} $colsize {
			#string cat %${size}s
			set _ %${size}s
		}]
		set fmt	"   [join $col_fmts { | }]"
		set col_count	[llength $col_fmts]

		# Output the row data
		foreach row $rows {
			set data	[list {*}$row {*}[lrepeat [- $col_count [llength $row]] --]]
			::bench::output notice [format $fmt {*}$data]
		}

		::bench::output notice ""
	}

	#>>>
}
proc run_benchmarks {dir args} { #<<<
	variable skipped
	variable run
	variable match
	variable skip
	variable output

	set match				*
	set skip				{}
	set relative			{}
	set display_mode		short
	set display_mode_args	{}
	set rundata				.

	set consume_args [list  \
		count {
			upvar 1 i i args args
			set from	$i
			incr i $count
			lrange $args $from [+ $from $count -1]
		} [namespace current] \
	]

	# Automatically save and compare with the previous run
	set args [list {*}{
		-relative last last
	} {*}$args]

	set i	0
	while {$i < [llength $args]} {
		lassign [apply $consume_args 1] next

		switch -- $next {
			-rundata {
				lassign [apply $consume_args 1] rundata
			}

			-load {
				lassign [apply $consume_args 1] load_script
				namespace eval :: $load_script
			}

			-match {
				lassign [apply $consume_args 1] match
			}

			-skip {
				lassign [apply $consume_args 1] skip
			}

			-relative {
				lassign [apply $consume_args 2] label rel_fn
				set rel_fn	[file join $rundata $rel_fn]
				if {[file readable $rel_fn]} {
					dict set relative $label [_readfile [file join $rundata $rel_fn]]
				}
			}

			-save {
				lassign [apply $consume_args 1] save_fn
				set save_fn	[file join $rundata $save_fn]
			}

			-display {
				lassign [apply $consume_args 1] display_mode
				set display_mode_args	{}
				while {[string index [lindex $args $i] 0] ni {"-" ""}} {
					lappend display_mode_args	[apply $consume_args 1]
				}
			}

			default {
				throw [list BENCH INVALID_ARG $next] \
					"Invalid argument: \"$next\""
			}
		}
	}

	timerate -calibrate {} 1000
	set stats	{}
	foreach f [glob -nocomplain -type f -dir $dir -tails *.bench] {
		try {
			namespace eval ::_bench_private {namespace path {::bench}}
			namespace eval ::_bench_private [list source [file join $dir $f]]
		} finally {
			namespace delete ::_bench_private
		}
	}

	set save [list {save_fn run} {
		set save_data	$run
		if {[file readable $save_fn]} {
			# If the save file already exists, merge this run's data with it
			# rather than replacing it (keeps old tests that weren't executed
			# in this run)
			set newkeys	[lmap {relname - -} $save_data {set relname}]
			set old		[_readfile $save_fn]
			foreach {relname reldesc relstats} $old {
				if {$relname in $newkeys} continue
				lappend save_data $relname $reldesc $relstats
			}
		}
		_writefile $save_fn $save_data
	} [namespace current]]

	apply $save [file join $rundata last] $run	;# Always save as "last", even if explicitly saving as something else too
	if {[info exists save_fn]} {
		apply $save $save_fn $run
	}

	foreach {name desc variant_stats} $run {
		set relative_stats	{}
		foreach {label relinfo} $relative {
			foreach {relname reldesc relstats} $relinfo {
				if {$relname eq $name} {
					lappend relative_stats $label $relstats
					break
				}
			}
		}
		try {
			display_bench $display_mode $name $desc $variant_stats $relative_stats {*}$display_mode_args
		} trap {TCL LOOKUP SUBCOMMAND} {errmsg options} {
			puts $options
			apply $output error "Invalid display mode: \"$display_mode\""
			exit 1
		} trap {TCL WRONGARGS} {errmsg options} {
			apply $output error "Invalid display mode params: $errmsg"
			exit 1
		}
	}
}

#>>>
}
# vim: ft=tcl foldmethod=marker foldmarker=<<<,>>> ts=4 shiftwidth=4
