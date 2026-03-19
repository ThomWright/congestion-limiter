# Usage: gnuplot -e "scenario='basic'" /path/to/plot.gnu
# Or:    gnuplot plot.gnu  (defaults to 'basic', must be run from simulator/)

if (!exists("scenario")) scenario = "basic"
datadir = "output/".scenario

set terminal pngcairo size 1200,900 enhanced font "sans,10"
set output datadir."/plot.png"

set multiplot layout 2,2 title scenario noenhanced font ",14"
set grid
set key outside right top

# Bin width for throughput/rejection histograms (seconds)
binwidth = 0.5
bin(x) = binwidth * floor(x / binwidth) + binwidth/2.0

# Compute x range from data: [0, max_time + one bin]
stats datadir."/requests.dat" using 1 nooutput
req_xmax = STATS_max + binwidth
set xrange [0:req_xmax]

# --- Panel 1: Throughput over time ---
set title "Throughput (0.5 s bins)"
set xlabel "Time (s)"
set ylabel "Requests / s"
set boxwidth binwidth
set style fill solid 0.6

# Stacked: draw total (rejected colour) first, then success+overload on top, then success on top.
# The exposed red = limiter rejections; exposed orange = server overload; blue = success.
plot datadir."/requests.dat" \
    using (bin($1)):(1.0/binwidth) \
    smooth freq with boxes lc rgb "#e84c4c" title "rejected", \
    datadir."/requests.dat" \
    using (bin($1)):((strcol(4) eq "success" || strcol(4) eq "overload") ? 1.0/binwidth : 0.0) \
    smooth freq with boxes lc rgb "#e8a84c" title "overload", \
    datadir."/requests.dat" \
    using (bin($1)):(strcol(4) eq "success" ? 1.0/binwidth : 0.0) \
    smooth freq with boxes lc rgb "#4c9be8" title "success"

# --- Panel 2: Non-success breakdown (0.5 s bins) ---
set title "Rejections and overload (0.5 s bins)"
set xlabel "Time (s)"
set ylabel "Requests / s"
unset yrange

plot datadir."/requests.dat" \
    using (bin($1)):((strcol(4) eq "client_rejected" || strcol(4) eq "server_rejected") ? 1.0/binwidth : 0.0) \
    smooth freq with lines lc rgb "#e84c4c" lw 2 title "rejected", \
    datadir."/requests.dat" \
    using (bin($1)):(strcol(4) eq "overload" ? 1.0/binwidth : 0.0) \
    smooth freq with lines lc rgb "#e8a84c" lw 2 title "overload"

# --- Panel 3: Concurrency limit over time ---
set title "Concurrency limit"
set xlabel "Time (s)"
set ylabel "Limit"
set yrange [0:*]
unset style
unset boxwidth

plot for [node in system("awk 'NR>1{print $2}' ".datadir."/snapshots.dat | sort -u")] \
    datadir."/snapshots.dat" \
    using (stringcolumn(2) eq node ? $1 : 1/0):3 \
    with lines lw 2 title node

# --- Panel 4: In-flight over time ---
set title "In-flight requests"
set xlabel "Time (s)"
set ylabel "In-flight"

plot for [node in system("awk 'NR>1{print $2}' ".datadir."/snapshots.dat | sort -u")] \
    datadir."/snapshots.dat" \
    using (stringcolumn(2) eq node ? $1 : 1/0):4 \
    with lines lw 2 title node

unset multiplot
print "Written ".datadir."/plot.png"
