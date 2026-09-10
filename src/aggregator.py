"""Real-time aggregation of order prices.

Keeps a running (streaming) average so that memory use stays constant no matter
how many orders arrive - only a count and a sum are held per key, never the
individual prices.

Welford's online algorithm is used for the variance so the standard deviation is
numerically stable over long runs.
"""
from dataclasses import dataclass, field


@dataclass
class RunningStats:
    """Count / mean / min / max / stddev maintained incrementally."""
    count: int = 0
    mean: float = 0.0
    _m2: float = 0.0          # sum of squared deviations from the running mean
    total: float = 0.0
    minimum: float = float("inf")
    maximum: float = float("-inf")

    def add(self, value: float) -> None:
        self.count += 1
        self.total += value
        delta = value - self.mean
        self.mean += delta / self.count
        self._m2 += delta * (value - self.mean)
        self.minimum = min(self.minimum, value)
        self.maximum = max(self.maximum, value)

    @property
    def stddev(self) -> float:
        return (self._m2 / (self.count - 1)) ** 0.5 if self.count > 1 else 0.0


@dataclass
class OrderAggregator:
    """Global running average plus a per-product breakdown."""
    overall: RunningStats = field(default_factory=RunningStats)
    per_product: dict[str, RunningStats] = field(default_factory=dict)

    def add(self, product: str, price: float) -> float:
        """Record one order and return the new overall running average."""
        self.overall.add(price)
        self.per_product.setdefault(product, RunningStats()).add(price)
        return self.overall.mean

    def snapshot_line(self) -> str:
        """One-line summary, printed after every processed order."""
        o = self.overall
        return (f"orders={o.count} avg={o.mean:8.2f} "
                f"min={o.minimum:7.2f} max={o.maximum:7.2f} "
                f"sd={o.stddev:6.2f} total={o.total:10.2f}")

    def report(self) -> str:
        """Full table, printed periodically and at shutdown."""
        if self.overall.count == 0:
            return "no orders aggregated yet"

        width = max((len(p) for p in self.per_product), default=7)
        width = max(width, 7)
        lines = [
            "-" * (width + 54),
            f"{'PRODUCT':<{width}} {'COUNT':>7} {'AVG':>10} "
            f"{'MIN':>9} {'MAX':>9} {'TOTAL':>12}",
            "-" * (width + 54),
        ]
        for product, s in sorted(self.per_product.items()):
            label = product if product else "(blank)"
            lines.append(f"{label:<{width}} {s.count:>7} {s.mean:>10.2f} "
                         f"{s.minimum:>9.2f} {s.maximum:>9.2f} {s.total:>12.2f}")
        o = self.overall
        lines.append("-" * (width + 54))
        lines.append(f"{'ALL':<{width}} {o.count:>7} {o.mean:>10.2f} "
                     f"{o.minimum:>9.2f} {o.maximum:>9.2f} {o.total:>12.2f}")
        lines.append("-" * (width + 54))
        return "\n".join(lines)
