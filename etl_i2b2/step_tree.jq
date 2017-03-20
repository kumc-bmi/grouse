#
# handy sort by process:
#  sort --stable --field-separator=, --key=1,1
#
# so:
# < log/grouse-detail.json jq -C -c -f step_tree.jq |
#      sort --stable --field-separator=, --key=1,1 | less -R
select(.step and .elapsed) |
 [.process, .event.task_hash, .asctime[11:],
  (.event["task_family"]),
  .event.script,
  .step, .do, .args.event,
  ((if (.do=="end") then { elapsed: [(.elapsed[1] | split("."))[0], .elapsed[2]] }
    else {start: .elapsed[0][11:]} end) +
   ((.args.lineno | numbers | {lineno: .}) // {}) +
   ((.result | numbers | {result: .}) // {}) +
   ((.args.chunk_num | numbers | {chunk_num: .}) // {}))
]