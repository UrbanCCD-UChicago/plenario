package require tcltest 2
namespace import tcltest::*

source [file dirname $argv0]/../library.tcl

makeFile "" "Portfile"
makeFile "" $output_file
makeDirectory $work_dir
set path [file dirname [file normalize $argv0]]

# Initial setup
load_variables $path
set_dir
port_index

proc univ_test {opt} {
    global output_file path portsrc bindir

    # Modify Porfile.in for variants.
    if {$opt ne "yes"} {
        # No universal variant
        exec sed "s/@option@/universal_variant\ no/" $path/Portfile.in > Portfile
    } else {
        # Add universal variant
        exec sed "s/@option@/default_variants\ +universal/" $path/Portfile.in > Portfile
    }
    port_clean $path

    # Build helping string
    set string "export PORTSRC=${portsrc} ; ${bindir}/port info --variants"

    exec sh -c $string > output 2>@1
    set var "variants:*"
    set line [get_line $path/$output_file $var]
    return $line
}


test universal {
    Regression test for universal variant.
} -body {
    univ_test "yes"
} -result "variants: universal"

test nouniversal {
    Regression test for no universal variant.
} -body {
    univ_test "no"
} -result "variants: "


cleanup
cleanupTests
