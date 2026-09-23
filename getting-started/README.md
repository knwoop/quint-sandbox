# Getting Started

A bank spec from the [Quint getting started guide](https://quint.sh/docs/getting-started).
It has three accounts with `deposit` and `withdraw` actions, and checks the invariant `no_negatives` (no balance ever goes below zero).

All commands below are run from the repository root.
See the [root README](../README.md) for setup.

## 1. Simulate and find the bug

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

## 2. Fix `withdraw`

`bank.qnt` in this folder already contains the fix:

```quint
action withdraw(account, amount) = all {
  balances.get(account) >= amount,
  balances' = balances.setBy(account, curr => curr - amount),
}
```

Running step 1 again now finishes with no violation.

## 3. Verify with the model checker

```sh
quint verify getting-started/bank.qnt --invariant=no_negatives
```

Expected: `[ok] No violation found`.
It checks up to 10 steps by default; `--max-steps` changes that.
