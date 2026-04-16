# YouTool + Bakunawa Mech Pebble Deployment

This repository is the public PebbleHost deployment mirror for the Discord bots that run on the
same PebbleHost instance.

Its job is simple:

- hold only deployment-safe bot files
- stay free of secrets and private live data
- provide the Git repository watched by PebbleHost

## Relationship To The Other Repos

- YouTool private source repo: `master` is the stable source-of-truth branch
- YouTool private working branch: `develop` is for in-progress changes before they are promoted to `master`
- legacy reference branch: `legacy-rust` preserves the outdated original Rust implementation
- Bakunawa Mech source is mirrored into `bots/bakunawa/`
- this repo: `main` is the deployment branch the remote PebbleHost bot pulls on restart

## Runtime Layout

- `bot.py` is the PebbleHost start file and multi-bot launcher.
- The root repository directory runs YouTool.
- `bots/bakunawa/` runs Bakunawa Mech.
- Each bot has its own `config/`, `data/`, `logs/`, `exports/`, and `import/` directory.
- Each bot must use a different Discord token.

## What Belongs Here

Include:

- `src/`
- `migrations/`
- `bot.py`
- `bots/bakunawa/src/`
- `bots/bakunawa/migrations/`
- `bots/bakunawa/bot.py`
- `requirements.txt`
- safe shared config assets

Do not include:

- `config/app.toml`
- `bots/bakunawa/config/app.toml`
- `.env*`
- `data/`
- `bots/*/data/`
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

## PebbleHost Settings

- Keep the Python start file set to `bot.py`.
- Keep the Git branch set to `main`.
- Store live tokens only in PebbleHost runtime config files or environment variables.
- If using config files, create both `config/app.toml` and `bots/bakunawa/config/app.toml` from their `.example` files.
- If using environment variables, set `YT_ASSIST_DISCORD_TOKEN` for YouTool and `BAKUNAWA_MECH_DISCORD_TOKEN` for Bakunawa Mech.
