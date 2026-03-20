# Usage: gnuplot -e "scenario='overload_high'" simulator/compare_grid.gnu
# Run from the repository root after running all 6 algorithms for the given scenario.
# Produces a 6-row × 3-col grid: one row per algorithm.
# Columns: stacked throughput bar | concurrency limit | latency heatmap

if (!exists("scenario")) scenario = "overload_high"
basedir = "output/".scenario
algos = "aimd windowed_aimd vegas windowed_vegas gradient windowed_gradient"

set terminal pngcairo size 1500,1800 enhanced font "sans,9"
set output basedir."/compare_grid.png"

# Heatmap palette and settings apply to all splot commands.
set palette defined (0 "#f0f0f0", 1 "#4c9be8", 4 "#e8a84c", 10 "#e84c4c")
set pm3d map
unset colorbox

# Compute x range once from the first algo so all rows share the same axis.
binwidth = 0.5
stats basedir."/aimd/requests.dat" using 1 nooutput
req_xmax = STATS_max + binwidth

tbin_w = 1.0
lbin_w = 10.0

set multiplot layout 6,3 margins 0.06,0.98,0.01,0.97 spacing 0.06,0.06

do for [i=1:words(algos)] {
    algo = word(algos, i)
    adir = basedir."/".algo

    # ------------------------------------------------------------------ #
    # Column 1: stacked throughput bar                                    #
    # ------------------------------------------------------------------ #
    set title algo noenhanced font ",10"
    set xlabel ""
    set ylabel "req/s" font ",8"
    set xrange [0:req_xmax]
    set yrange [0:*]
    set grid
    set key top right inside font ",7" maxrows 2
    set style fill solid 0.6
    set boxwidth binwidth

    plot adir."/requests.dat" \
        using (binwidth*floor($1/binwidth)+binwidth/2):(1.0/binwidth) \
        smooth freq with boxes lc rgb "#e84c4c" title "c-rej", \
        adir."/requests.dat" \
        using (binwidth*floor($1/binwidth)+binwidth/2):(strcol(4) ne "client_rejected" ? 1.0/binwidth : 0.0) \
        smooth freq with boxes lc rgb "#9b4ce8" title "s-rej", \
        adir."/requests.dat" \
        using (binwidth*floor($1/binwidth)+binwidth/2):((strcol(4) eq "success"||strcol(4) eq "overload") ? 1.0/binwidth : 0.0) \
        smooth freq with boxes lc rgb "#e8a84c" title "ovld", \
        adir."/requests.dat" \
        using (binwidth*floor($1/binwidth)+binwidth/2):(strcol(4) eq "success" ? 1.0/binwidth : 0.0) \
        smooth freq with boxes lc rgb "#4c9be8" title "ok"

    set style fill empty
    set boxwidth

    # ------------------------------------------------------------------ #
    # Column 2: concurrency limit                                         #
    # ------------------------------------------------------------------ #
    set title ""
    set ylabel "limit" font ",8"
    set yrange [0:*]
    unset key

    plot sprintf("< awk 'NR>1 && $2==\"client_0\"{print $1,$3}' %s/snapshots.dat", adir) \
        using 1:2 with lines lw 1.5 lc rgb "#4c9be8" notitle

    # ------------------------------------------------------------------ #
    # Column 3: latency heatmap                                           #
    # ------------------------------------------------------------------ #
    set title ""
    set ylabel "latency (ms)" font ",8"
    set yrange [0:*]
    set cbrange [0:*]

    heatmap_cmd = sprintf( \
        "< awk 'NR>1 && ($4==\"success\"||$4==\"overload\"){t=int($1/%g); l=int($3*1000/%g); cnt[t,l]++; if(t>tmax)tmax=t; if(l>lmax)lmax=l} END{for(ti=0;ti<=tmax;ti++){for(li=0;li<=lmax;li++) print (ti+0.5)*%g,(li+0.5)*%g,(((ti,li) in cnt)?cnt[ti,li]:0); print\"\"}}' %s/requests.dat", \
        tbin_w, lbin_w, tbin_w, lbin_w, adir)

    splot heatmap_cmd using 1:2:3
}

unset multiplot
print "Written ".basedir."/compare_grid.png"
