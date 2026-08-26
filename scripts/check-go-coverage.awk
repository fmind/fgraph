# Go's displayed total is rounded, so validate the profile's exact covered and
# total statement counts instead of parsing `go tool cover -func` output.
NR == 1 { next }

NF != 3 || $2 !~ /^[0-9]+$/ || $3 !~ /^[0-9]+$/ {
  invalid = 1
  next
}

{
  total += $2
  if ($3 > 0) covered += $2
}

END {
  if (invalid || total == 0) {
    print "invalid or empty Go coverage profile"
    exit 1
  }

  coverage = 100 * covered / total
  printf "total coverage %.3f%% (%d/%d statements)\n", coverage, covered, total
  if (100 * covered < 95 * total) {
    print "total coverage is below the exact 95% gate"
    exit 1
  }
}
