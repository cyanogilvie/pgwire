# Inline all the c source pulled in by [_read_c somefile.c]

lassign $argv in out

proc readfile fn {
	set h	[open $fn r]
	try {read $h} finally {close $h}
}

proc writefile {fn str} {
	set h	[open $fn w]
	try {puts -nonewline $h $str} finally {close $h}
}

set dir			[file dirname [file normalize $in]]
set script		[readfile $in]
set outscript	{}
set from		0
foreach {m c_fn_idx} [regexp -all -inline -indices {\[_read_c ([a-z_]+\.c)\]} $script] {
	set pref	[string range $script $from [lindex $m 0]-1]
	set from	[expr {[lindex $m 1]+1}]
	set c_fn	[string range $script {*}$c_fn_idx]
	#append outscript $pref [list [readfile [file join $dir $c_fn]]]
	append outscript $pref "{[readfile [file join $dir $c_fn]]}"
	puts "Inlined $c_fn"
}
append outscript	[string range $script $from end]

writefile $out $outscript
