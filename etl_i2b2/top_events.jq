#!/usr/bin/jq
#
# Usage:
#   jq -c -f top_events.jq log/grouse-detail.json
# or:
#   jq -c -C -f top_events.jq log/grouse-detail.json |less -R
#
# ref: jq 1.5 Manual
#   https://stedolan.github.io/jq/manual/v1.5/

select(.do=="end") | select(.args|objects|.step|length == 1) |
    [.asctime, (.elapsed[1] | split("."))[0], .args.event,
     {script: .context.script, upload_id: .context.upload_id}]
