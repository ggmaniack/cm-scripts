# cm-scripts

Tampermonkey scripts and helper libraries for Cardmarket.

## Contents

| File | Purpose | Notes |
| --- | --- | --- |
| [`refactored_cardmarket.js`](https://github.com/ggmaniack/cm-scripts/raw/main/refactored_cardmarket.js) | Main Tampermonkey userscript. Preloads Cardmarket price-guide data, shows ratios immediately, and loads graphs on demand. | Install this one in Tampermonkey. |
| `draggable_box.js` | Helper library for draggable hover/click popup boxes. | Loaded automatically by `refactored_cardmarket.js` via `@require`. |
| `cardmarket_path_utils.js` | Helper library for locale/game-aware Cardmarket URL parsing. | Loaded automatically by `refactored_cardmarket.js` via `@require`. |

## Installation

1. Install the [Tampermonkey browser extension](https://www.tampermonkey.net/).
2. Open the raw install URL for [`refactored_cardmarket.js`](https://github.com/ggmaniack/cm-scripts/raw/main/refactored_cardmarket.js).
3. When Tampermonkey opens the installation prompt, confirm the install.
4. Open the supported Cardmarket pages and use the script there.

## Script details

### `refactored_cardmarket.js`

The main Cardmarket enhancement script. It preloads price-guide data, shows ratios immediately, and loads graphs on demand. Its helper libraries are fetched automatically through Tampermonkey `@require` metadata, so this is the only file you need to install.

- Install: <https://github.com/ggmaniack/cm-scripts/raw/main/refactored_cardmarket.js>

### Helper libraries

`draggable_box.js` and `cardmarket_path_utils.js` are helper libraries used by `refactored_cardmarket.js`. Do not install them separately in Tampermonkey.

## Updates

`refactored_cardmarket.js` includes update metadata, and its helper libraries are loaded through `@require`, so Tampermonkey can refresh the full script stack through normal update checks.
