# quint-sandbox

A playground for learning [Quint](https://quint.sh).

## Setup

Install Quint:

```sh
npm i -g @informalsystems/quint
quint --version   # 0.32.0 at the time of writing
```

The first `quint run` downloads the Rust evaluator to `~/.quint/` by itself.

`quint verify` runs the Apalache model checker, which needs Java 17 or later.
If you don't have a JDK installed, you can run it with a temporary one through mise:

```sh
mise exec java@temurin-21 -- quint verify <file>.qnt --invariant=<name>
```

The first `quint verify` also downloads Apalache.

## Layout

Each topic gets its own folder, with a README describing how to run it.
Commands in those READMEs are run from the repository root.

- [getting-started](getting-started/) — the bank spec from the official getting started guide

`_apalache-out/` holds the output of `quint verify` and is gitignored.
