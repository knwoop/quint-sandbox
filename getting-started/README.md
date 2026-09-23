# Getting Started

A bank spec from the [Quint getting started guide](https://quint.sh/docs/getting-started).
It has three accounts with `deposit` and `withdraw` actions, and checks the invariant `no_negatives` (no balance ever goes below zero).

All commands below are run from the repository root.

## 1. Install Quint

```sh
npm i -g @informalsystems/quint
quint --version   # 0.32.0 at the time of writing
```

The first `quint run` downloads the Rust evaluator to `~/.quint/` by itself.

## 2. Simulate and find the bug

The original tutorial's `withdraw` has no balance check:

```quint
action withdraw(account, amount) = {
  balances' = balances.setBy(account, curr => curr - amount)
}
```

With that version, simulation finds a violation:

```sh
quint run getting-started/bank.qnt --invariant=no_negatives
quint run getting-started/bank.qnt --invariant=no_negatives --mbt   # also shows which action ran and what values were picked
```

Expected: `error: Invariant violated`.

## 3. Fix `withdraw`

`bank.qnt` in this folder already contains the fix:

```quint
action withdraw(account, amount) = all {
  balances.get(account) >= amount,
  balances' = balances.setBy(account, curr => curr - amount),
}
```

Running step 2 again now finishes with no violation.

## 4. Verify with the model checker

`quint verify` runs Apalache, which needs Java 17 or later.

```sh
# with a JDK already installed
quint verify getting-started/bank.qnt --invariant=no_negatives

# without one: use a temporary JDK through mise
mise exec java@temurin-21 -- quint verify getting-started/bank.qnt --invariant=no_negatives
```

Expected: `[ok] No violation found`.
It checks up to 10 steps by default; `--max-steps` changes that.
The first run also downloads Apalache.
Its output goes to `_apalache-out/`, which is gitignored.
