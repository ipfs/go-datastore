#!/bin/sh

sort -nk2 res | awk '$1 == "direct" {$2 = "direct"}; 1' > res2

gnuplot <<EOF
set terminal png size 800,500
set grid lw 0.25
set output "plot.png"
set style fill solid 0.25 border -1
set style boxplot outliers pointtype 7
set style data boxplot
set title "flatfs query key-only 10,000 entries"
set ylabel "time (ms)"
set xlabel "buffer size"

plot "res2" using (1):3:(0.5):2 notitle
EOF


