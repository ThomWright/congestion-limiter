---- MODULE multi_pool_tlc ----
\* TLC wrapper for multi_pool. Provides sequence constants that
\* can't be defined inline in TLC's .cfg format.

EXTENDS multi_pool

const_weights == <<1, 3>>

====
