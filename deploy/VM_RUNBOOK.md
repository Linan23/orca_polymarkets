# VM Runbook

The canonical VM deployment guide is [../docs/DEPLOY_VM.md](../docs/DEPLOY_VM.md).

Quick deploy:

```bash
cd /home/lynchej/orca_polymarkets
git pull origin main
scripts/setup_vm.sh
```

Routine checks:

```bash
sudo systemctl status orca-api.service
sudo systemctl status orca-frontend.service
sudo systemctl status orca-ingest-live.service
sudo systemctl status orca-analytics-refresh.service
curl -fsS http://localhost:8001/health
```

Keep secrets in `/etc/orca.env`, not in Git.
