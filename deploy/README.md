# deploy/

`littledevil-recorder.service` is the systemd unit for the recorder,
tracked here so the production configuration has a source of truth in
version control. It was not previously tracked anywhere in this repo -- the
live unit on the production EC2 instance (i-01aedcf6d28b63813) was created
out of band and diverged from what's here.

This repo's own guardrails (and the Claude Code session that authored this
file) do not apply this unit to the production VM directly. Deploying it is
a manual step for Omar:

```bash
sudo cp deploy/littledevil-recorder.service /etc/systemd/system/littledevil-recorder.service
sudo systemctl daemon-reload
sudo systemctl restart littledevil-recorder
```

Restarting the live service is exactly the kind of production change this
repo's automated tooling is not permitted to make on its own -- do this
deliberately, once the storage.py fix (see dev-journal.md,
2026-09-17 entry) has been deployed and reviewed, not as a reflexive "apply
the new unit file" step.

Adjust `MemoryMax` in the unit if the instance size changes.
