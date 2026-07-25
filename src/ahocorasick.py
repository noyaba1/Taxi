"""
ahocorasick.py
==============
Multi-pattern matching over sequences of H3 CELLS (tokens, not characters).

WHY
---
Methods A and C both need the same thing: given a few thousand candidate routes,
how many trips contain each one? The obvious loop --

    for trip in trips:            # 1.71M
        for cand in candidates:   # up to 3000
            if cand in trip: ...

-- is ~5x10^9 Python substring scans and was the single slowest thing in the
pipeline. Aho-Corasick answers ALL candidates in ONE pass over each trip:
O(len(trip) + matches) instead of O(len(trip) x n_candidates).

Matching on TOKENS rather than on a joined string also removes a whole class of
bug: a delimiter-joined string can match across a cell boundary if the delimiter
handling is ever wrong, whereas a token automaton simply cannot.

Build once per Spark partition (it is picklable, but rebuilding beats shipping a
large object), then stream every trip through it.
"""
from __future__ import annotations

from collections import deque


class Automaton:
    """Aho-Corasick over hashable tokens. Patterns are sequences of tokens."""

    __slots__ = ("goto", "fail", "out", "n_patterns")

    def __init__(self, patterns):
        self.goto: list[dict] = [{}]
        self.fail: list[int] = [0]
        self.out: list[set] = [set()]
        self.n_patterns = 0

        for pid, pattern in enumerate(patterns):
            self.n_patterns += 1
            node = 0
            for tok in pattern:
                nxt = self.goto[node].get(tok)
                if nxt is None:
                    nxt = len(self.goto)
                    self.goto.append({})
                    self.fail.append(0)
                    self.out.append(set())
                    self.goto[node][tok] = nxt
                node = nxt
            self.out[node].add(pid)

        # BFS to build failure links; a node inherits the outputs of its fail
        # target so `matches` never has to walk the fail chain to collect hits.
        queue = deque()
        for nxt in self.goto[0].values():
            self.fail[nxt] = 0
            queue.append(nxt)
        while queue:
            cur = queue.popleft()
            for tok, nxt in self.goto[cur].items():
                queue.append(nxt)
                f = self.fail[cur]
                while f and tok not in self.goto[f]:
                    f = self.fail[f]
                target = self.goto[f].get(tok, 0)
                self.fail[nxt] = 0 if target == nxt else target
                self.out[nxt] |= self.out[self.fail[nxt]]

    def matches(self, tokens) -> set:
        """Ids of every pattern occurring anywhere in `tokens` (deduped)."""
        if not tokens:
            return set()
        node = 0
        found: set = set()
        for tok in tokens:
            while node and tok not in self.goto[node]:
                node = self.fail[node]
            node = self.goto[node].get(tok, 0)
            if self.out[node]:
                found |= self.out[node]
        return found


def containment_support(trips_rdd, patterns):
    """
    {pattern_id: number of DISTINCT trips containing it}.

    `trips_rdd` is an RDD of token sequences. The automaton is built once per
    partition, so the broadcast payload stays small (the pattern list) and the
    per-trip cost is linear in the trip length.
    """
    def _per_partition(rows):
        auto = Automaton(patterns)
        for tokens in rows:
            for pid in auto.matches(tokens):
                yield pid, 1

    return trips_rdd.mapPartitions(_per_partition).reduceByKey(lambda a, b: a + b)
