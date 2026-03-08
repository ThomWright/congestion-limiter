---- MODULE Apalache ----
\* Minimal stub for TLC compatibility with Quint-compiled TLA+ specs.
\* Only defines the operators we actually use. Full versions at:
\* https://github.com/apalache-mc/apalache/tree/main/src/tla

\* Apalache uses := for assignment; in TLC it's just equality.
a := b == a = b

\* Apalache's fold over a set.
RECURSIVE ApaFoldSet(_, _, _)
ApaFoldSet(op(_, _), base, s) ==
  IF s = {} THEN base
  ELSE LET x == CHOOSE x \in s : TRUE
       IN ApaFoldSet(op, op(base, x), s \ {x})

====
