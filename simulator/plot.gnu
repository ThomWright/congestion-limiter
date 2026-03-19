# Usage: gnuplot -e "scenario='basic'" /path/to/plot.gnu
# Or:    gnuplot plot.gnu  (defaults to 'basic', must be run from simulator/)

if (!exists("scenario")) scenario = "basic"
datadir = "output/".scenario

set terminal pngcairo size 1200,1350 enhanced font "sans,10"
set output datadir."/plot.png"

set multiplot layout 3,2 title scenario noenhanced font ",14"
set grid
set key outside right top

# Bin width for throughput/rejection histograms (seconds)
binwidth = 0.5
bin(x) = binwidth * floor(x / binwidth) + binwidth/2.0

# Compute x range from data: [0, max_time + one bin]
stats datadir."/requests.dat" using 1 nooutput
req_xmax = STATS_max + binwidth
set xrange [0:req_xmax]
set yrange [0:*]

# --- Panel 1: Throughput over time ---
set title "Throughput (0.5s bins)"
set xlabel "Time (s)"
set ylabel "Requests / s"
set boxwidth binwidth
set style fill solid 0.6

# Stacked: draw total first, then progressively narrower bands on top.
# Exposed red = client rejected; exposed purple = server rejected; exposed orange = overload; blue = success.
plot datadir."/requests.dat" \
    using (bin($1)):(1.0/binwidth) \
    smooth freq with boxes lc rgb "#e84c4c" title "client rejected", \
    datadir."/requests.dat" \
    using (bin($1)):(strcol(4) ne "client_rejected" ? 1.0/binwidth : 0.0) \
    smooth freq with boxes lc rgb "#9b4ce8" title "server rejected", \
    datadir."/requests.dat" \
    using (bin($1)):((strcol(4) eq "success" || strcol(4) eq "overload") ? 1.0/binwidth : 0.0) \
    smooth freq with boxes lc rgb "#e8a84c" title "overload", \
    datadir."/requests.dat" \
    using (bin($1)):(strcol(4) eq "success" ? 1.0/binwidth : 0.0) \
    smooth freq with boxes lc rgb "#4c9be8" title "success"

# --- Panel 2: Non-success breakdown by outcome (0.5s bins) ---
set title "Rejections and overload by outcome (0.5s bins)"
set xlabel "Time (s)"
set ylabel "Requests / s"

prebin(outcome) = sprintf("< awk -v bw=%g -v xmax=%g -v outcome=%s 'NR>1 && $4==outcome{b=int($1/bw); c[b]++} END{n=int(xmax/bw)+1; for(i=0;i<=n;i++) print (i+0.5)*bw, (c[i]+0)/bw}' %s/requests.dat", binwidth, req_xmax, outcome, datadir)
plot prebin("client_rejected") using 1:2 with lines lw 2 title "client rejected", \
     prebin("server_rejected") using 1:2 with lines lw 2 title "server rejected", \
     prebin("overload")        using 1:2 with lines lw 2 title "overload"

# --- Panel 3: Concurrency limit over time ---
set title "Concurrency limit"
set xlabel "Time (s)"
set ylabel "Limit"
unset style
unset boxwidth

plot for [node in system("awk 'NR>1{print $2}' ".datadir."/snapshots.dat | sort -u")] \
    sprintf("< awk '$2==\"%s\"{print $1,$3}' %s/snapshots.dat", node, datadir) \
    using 1:2 with lines lw 2 title node noenhanced

# --- Panel 4: In-flight over time ---
set title "In-flight requests"
set xlabel "Time (s)"
set ylabel "In-flight"

plot for [node in system("awk 'NR>1{print $2}' ".datadir."/snapshots.dat | sort -u")] \
    sprintf("< awk '$2==\"%s\"{print $1,$4}' %s/snapshots.dat", node, datadir) \
    using 1:2 with lines lw 2 title node noenhanced

# --- Panel 5: Latency heatmap ---
# Bins: 1 s time bins × 10 ms latency buckets. Only completed requests (success + overload).
set title "Latency heatmap (completed requests)"
set xlabel "Time (s)"
set ylabel "Latency (ms)"
set yrange [0:*]
set cbrange [0:*]
set palette defined (0 "#f0f0f0", 1 "#4c9be8", 4 "#e8a84c", 10 "#e84c4c")
unset key

tbin_w = 1.0
lbin_w = 10.0

# Output a complete t×l grid (zeroes for empty bins) with blank lines between time rows,
# so pm3d can render it as a proper rectangular heatmap without diagonal artifacts.
heatmap_cmd = sprintf("< awk 'NR>1 && ($4==\"success\" || $4==\"overload\") {t=int($1/%g); l=int($3*1000/%g); cnt[t,l]++; if(t>tmax)tmax=t; if(l>lmax)lmax=l} END {for(ti=0;ti<=tmax;ti++){for(li=0;li<=lmax;li++){print (ti+0.5)*%g,(li+0.5)*%g,(((ti,li) in cnt)?cnt[ti,li]:0)}; print\"\"}}' %s/requests.dat", tbin_w, lbin_w, tbin_w, lbin_w, datadir)

set pm3d map
splot heatmap_cmd using 1:2:3

# --- Panel 6: Database queue depth (skipped if no database in this scenario) ---
has_db = int(system("awk 'NR>1 && $2==\"database\"{found=1; exit} END{print found+0}' " . datadir . "/snapshots.dat"))
if (has_db) {
    set title "Database queue depth"
    set xlabel "Time (s)"
    set ylabel "Queued requests"
    set key
    plot sprintf("< awk 'NR>1 && $2==\"database\"{b=int($1/2); q=$4-$3; s[b]+=(q>0?q:0); c[b]++} END{for(b in s) print b*2+1,s[b]/c[b]}' %s/snapshots.dat | sort -n", datadir) \
        using 1:2 with lines lw 2 lc rgb "#e84c4c" notitle
}

unset multiplot
print "Written ".datadir."/plot.png"
