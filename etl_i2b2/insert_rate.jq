#!/usr/bin/jq
#
# Usage:
#   jq -f insert_rate.jq log/grouse-detail.json
# or:
#   jq --compact-output --color-output --from-file insert_rate.jq log/grouse-detail.json
#
# ref: jq 1.5 Manual
#   https://stedolan.github.io/jq/manual/v1.5/

select(.args|objects|.rowcount) |
    [.asctime, .context.task_hash, .context.script, .step,
     (.args.into | split("."))[-1], .args.chunk_num,
     {elapsed: (.elapsed[1] | split("."))[0],
      rowcount: .args.rowcount,
      krowpersec: (.args.rowcount / .elapsed[2] * 1000 * 60 + 0.5 | floor)}]
