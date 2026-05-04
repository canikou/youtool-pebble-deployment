# Agent Notes

This repository is the private development source for Bakunawa Mech Bot.

## Deployment Flow

The live watched deployment repository is:

- Local path: `D:\Coding Projects\youtool-pebble-deployment`
- GitHub: `canikou/youtool-pebble-deployment`
- Deployment subdirectory: `bots/bakunawa/`

When the user asks to work on "Bakunawa Mech Bot", make code changes here first:

- Local path: `D:\Coding Projects\bakunawa-mech-bot`
- GitHub: `canikou/bakunawa-mech-bot`

After changes are tested and ready for deployment, mirror deployment-safe files into the watched deployment repo:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\mirror-to-deployment.ps1
```

Then commit and push both repositories when appropriate:

1. Commit and push this private source repo.
2. Review `D:\Coding Projects\youtool-pebble-deployment` status.
3. Commit and push `youtool-pebble-deployment` `main` so the remote host can pick up the update.

Never commit live secrets or runtime data:

- `.env`
- `.env.*`
- `config/app.toml`
- `data/`
- `logs/`
- `exports/`
- `import/`

## Local Setup

Use Python 3.12.

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Run compile smoke checks with:

```powershell
.\.venv\Scripts\python.exe -m compileall -q .
```
