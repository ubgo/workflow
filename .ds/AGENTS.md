# docsync rules for an AI writer and reviewer

The tool knows which sentence depends on which block and whether the block changed. Read only what `check` lists; acks are recorded against the hash.

Writing:
1. Never cite a path and line. `ds def <file>#<symbol>` for the block, then `ds:block?id=…`.
2. Never paste code. Write a `ds:block` directive; the build renders it.
3. Never type a fact. Run `ds facts`; cite with `ds:cfg` if it exists; otherwise define once with an inline `ds:def`, then cite.
4. External links go through `ds:url`. Behaviour claims cite the test with `assert=true` when one exists.
5. Add `covers` for every id a new page introduces.
6. Start a session with `ds map`. When editing a page, start from `ds context <doc> --budget N --since ack`; nothing outside it needs rereading. Use `find`, `read`, `locate` instead of grep and file reads.
7. Run `ds check` before declaring done. Run `ds impact` before proposing a code change that touches defined blocks.
8. After a code change, `ds check --json` is the complete work list.
9. Before deleting or renaming code, `ds why <id>`, and handle dependents in the same change.

Reviewing:
1. `ds check --json` is the whole list.
2. For each `unacked`, read the sentence, the class, and the diff. Still true: `ack --note`. Not true: edit, then `ack --note`. Never ack a page wholesale.
3. For each `broken`, decide from the diff whether the block was deleted or moved without its def; re-add the def or rewrite and remove the reference.
4. Produce the patch and stop; a person or an explicitly delegated agent records acks (`ack --agent --delegated-by <human>`).
5. Leave anything needing a human unacked and say why.
