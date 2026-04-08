PebbleHost upload package for YTAssist.

This folder is prepared to match PebbleHost's Python bot layout:
- requirements.txt at the root
- bot.py as the start file
- pebble-python-config.json pinned to Python 3.12

Recommended upload/use:
1. Open PebbleHost File Manager for the bot.
2. Delete the default root requirements.txt and bot.py if PebbleHost created them.
3. Upload the contents of this folder, or upload the zip made from this folder.
4. In PebbleHost Loader, set Bot Start File to bot.py.
5. Start the bot and watch Console/Chat for startup logs.

Included runtime data:
- config/ with the live app.toml and bot content files
- data/ with the live SQLite database and attachment files
- exports/ with the latest export backup

Intentionally omitted to keep the upload practical:
- old logs
- the large import staging file from the local machine

The bot will recreate logs/ and use import/ automatically if they are missing, but both
directories are included as empty folders here for convenience.
