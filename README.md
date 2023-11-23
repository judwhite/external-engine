# Lichess External Engine

Use an engine outside of the browser for
[analysis on lichess.org](https://lichess.org/analysis).

Local Stockfish, Stockfish on your friend's computer, other UCI engines maybe, etc.

API: https://lichess.org/api#tag/External-engine.

## Quick start

1. Create a token at https://lichess.org/account/oauth/token/create?scopes[]=engine:read&scopes[]=engine:write.

2. Run:

   ```
   go build && LICHESS_API_TOKEN=lip_*** ./external-engine --engine /usr/bin/stockfish
   ```

3. Visit https://lichess.org/analysis.

4. Open the ~~hamburger~~ gear menu (⚙️) in the top right and select **External Stockfish** or **External Engine**
   from the list of engines.

## What the fork?

This is a fork of https://github.com/lichess-org/external-engine, a great tool I've used for years (including alpha 1).
Big ups to Lichess for even making this possible.

This version aims to handle network hiccups better and let you flip through a game like a maniac with the engine on,
no problem.

I had times when no lines would show up in the analysis box, usually after some frantic piece movements and throwing
a lot of "start, stop, no wait, change to this position" at it. The original script doesn't wait for the output
to finish for a given position (you can receive `info` and `bestmove` after you send `stop`!), so some crossing of
streams could happen which made it look like random missing data. Anyway, that should be fixed, hopefully.
