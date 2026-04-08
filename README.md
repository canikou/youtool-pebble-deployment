# YouTool Pebble Deployment

This repository is the public PebbleHost deployment mirror for the main private source repo at
[canikou/youtool-bot](https://github.com/canikou/youtool-bot).

Its job is simple:

- hold only deployment-safe bot files
- stay free of secrets and private live data
- provide the Git repository watched by PebbleHost

## Relationship To The Other Repos

- private source repo: `master` is the stable source-of-truth branch
- private working branch: `develop` is for in-progress changes before they are promoted to `master`
- legacy reference branch: `legacy-rust` preserves the outdated original Rust implementation
- this repo: `main` is the deployment branch the remote PebbleHost bot pulls on restart

## What Belongs Here

Include:

- `src/`
- `migrations/`
- `bot.py`
- `requirements.txt`
- safe shared config assets

Do not include:

- `config/app.toml`
- `.env*`
- `data/`
- `logs/`
- `exports/`
- `import/`
- private tokens, credentials, or workstation-only artifacts

## Deployment Flow

1. Make and test changes in the private source repo.
2. Promote stable changes into the private repo's `master` branch.
3. Mirror only deployment-safe files into this repo.
4. Push this repo's `main` branch.
5. Restart the PebbleHost bot so Git Management pulls the latest deployment snapshot.
