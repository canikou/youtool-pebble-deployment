PebbleHost upload package for YouTool and Bakunawa Mech.

This folder is prepared to match PebbleHost's Python bot layout:
- requirements.txt at the root
- bot.py as the start file and multi-bot launcher
- pebble-python-config.json pinned to Python 3.12

Recommended upload/use:
1. Open PebbleHost File Manager for the bot.
2. Delete the default root requirements.txt and bot.py if PebbleHost created them.
3. Upload the contents of this folder, or upload the zip made from this folder.
4. In PebbleHost Loader, set Bot Start File to bot.py.
5. Add both runtime config files on PebbleHost:
   - config/app.toml for YouTool
   - bots/bakunawa/config/app.toml for Bakunawa Mech
6. Start the bot and watch Console/Chat for startup logs.

The root bot.py starts both bots as separate Python processes:
- YouTool runs from the repository root.
- Bakunawa Mech runs from bots/bakunawa.

Each bot keeps its own config, data, logs, exports, imports, database, stop file, and
Discord token. Do not reuse the same token in both config files.

If you prefer environment variables instead of writing tokens into app.toml, use:
- YT_ASSIST_DISCORD_TOKEN for YouTool
- BAKUNAWA_MECH_DISCORD_TOKEN for Bakunawa Mech

Included runtime data:
- config/ with safe shared content files and app.toml.example
- bots/bakunawa/config/ with Bakunawa Mech content files and app.toml.example

Intentionally omitted:
- config/app.toml
- bots/bakunawa/config/app.toml
- data/
- bots/bakunawa/data/
- logs/
- exports/
- import/
- private tokens, credentials, and live databases

The launcher creates data/, logs/, exports/, and import/ directories for each bot when
it starts.
