# Usage: gnuplot -e "scenario='overload_high'" simulator/compare.gnu
# Run from the repository root after running all 6 algorithms for the given scenario.
# Reads output/<scenario>/{algo}/requests.dat and snapshots.dat.

if (!exists("scenario")) scenario = "overload_high"
basedir = "output/".scenario
algos = "aimd windowed_aimd vegas windowed_vegas gradient windowed_gradient"

set terminal pngcairo size 1400,1050 enhanced font "sans,10"
set output basedir."/compare.png"
set multiplot layout 3,2 title scenario." — algorithm comparison" noenhanced font ",14"
set grid
set key outside right top

binwidth = 1.0
set yrange [0:*]

# One colour per algorithm: aimd, windowed_aimd, vegas, windowed_vegas, gradient, windowed_gradient
color(i) = word("#4c9be8 #1a5fa8 #4cad4c #2a6b2a #e8a84c #b06820", i)

# Zero-filled bin command: outputs 0 for every bin from 0 to the max observed time.
# Absent bins plot as zero rather than being skipped, so lines drop correctly to the baseline.
# outcome is matched against column 4; use "" to match all rows.
zerofill(outcome, adir) = sprintf( \
    "< awk -v bw=%g -v o='%s' 'NR>1{if($1>tmax)tmax=$1} NR>1 && (o==\"\" || $4==o){b=int($1/bw); c[b]++} END{n=int(tmax/bw); for(i=0;i<=n;i++) print (i+0.5)*bw,(i in c ? c[i]/bw : 0)}' %s/requests.dat", \
    binwidth, outcome, adir)

# --- Panel 1: Success throughput ---
set title "Success throughput (1s bins)"
set xlabel "Time (s)"
set ylabel "Requests / s"
plot for [i=1:words(algos)] zerofill("success", basedir."/".word(algos, i)) \
    using 1:2 with lines lw 2 lc rgb color(i) title word(algos, i) noenhanced

# --- Panel 2: Client rejection rate ---
set title "Client rejection rate (1s bins)"
set xlabel "Time (s)"
set ylabel "Requests / s"
plot for [i=1:words(algos)] zerofill("client_rejected", basedir."/".word(algos, i)) \
    using 1:2 with lines lw 2 lc rgb color(i) title word(algos, i) noenhanced

# --- Panel 3: Concurrency limit ---
# For multi-client scenarios (e.g. fairness) client_0 is one representative partition.
set title "Concurrency limit (client_0, 1s bins)"
set xlabel "Time (s)"
set ylabel "Limit"
plot for [i=1:words(algos)] \
    sprintf("< awk -v bw=%g 'NR>1 && $2==\"client_0\"{b=int($1/bw); s[b]+=$3; c[b]++} END{for(b in c) print (b+0.5)*bw, s[b]/c[b]}' %s/%s/snapshots.dat | sort -n", \
        binwidth, basedir, word(algos, i)) \
    using 1:2 with lines lw 2 lc rgb color(i) title word(algos, i) noenhanced

# --- Panel 4: In-flight requests ---
set title "In-flight requests (client_0, 1s bins)"
set xlabel "Time (s)"
set ylabel "In-flight"
plot for [i=1:words(algos)] \
    sprintf("< awk -v bw=%g 'NR>1 && $2==\"client_0\"{b=int($1/bw); s[b]+=$4; c[b]++} END{for(b in c) print (b+0.5)*bw, s[b]/c[b]}' %s/%s/snapshots.dat | sort -n", \
        binwidth, basedir, word(algos, i)) \
    using 1:2 with lines lw 2 lc rgb color(i) title word(algos, i) noenhanced

# --- Panel 5: Mean latency of completed requests ---
# Not zero-filled: no completed requests in a bin means no data point, not zero latency.
set title "Mean latency — completed requests (1s bins)"
set xlabel "Time (s)"
set ylabel "Latency (ms)"
plot for [i=1:words(algos)] \
    sprintf("< awk -v bw=%g 'NR>1 && ($4==\"success\"||$4==\"overload\"){b=int($1/bw); s[b]+=$3; c[b]++} END{for(b in c) print (b+0.5)*bw, s[b]/c[b]*1000}' %s/%s/requests.dat | sort -n", \
        binwidth, basedir, word(algos, i)) \
    using 1:2 with lines lw 2 lc rgb color(i) title word(algos, i) noenhanced

# --- Panel 6: Overload rate ---
set title "Overload rate (1s bins)"
set xlabel "Time (s)"
set ylabel "Requests / s"
plot for [i=1:words(algos)] zerofill("overload", basedir."/".word(algos, i)) \
    using 1:2 with lines lw 2 lc rgb color(i) title word(algos, i) noenhanced

unset multiplot
print "Written ".basedir."/compare.png"
