"""Fibonacci number utilities."""


def fibonacci(n: int) -> int:
    """Return the n-th Fibonacci number (0-indexed): F(0)=0, F(1)=1.

    Uses an iterative approach, O(n) time and O(1) space.

    Raises:
        TypeError: if n is not an int (bools are rejected too).
        ValueError: if n is negative.
    """
    if isinstance(n, bool) or not isinstance(n, int):
        raise TypeError("n must be an integer")
    if n < 0:
        raise ValueError("n must be non-negative")

    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b
    return a


def fibonacci_sequence(count: int) -> list[int]:
    """Return a list of the first `count` Fibonacci numbers."""
    if isinstance(count, bool) or not isinstance(count, int):
        raise TypeError("count must be an integer")
    if count < 0:
        raise ValueError("count must be non-negative")

    seq = []
    a, b = 0, 1
    for _ in range(count):
        seq.append(a)
        a, b = b, a + b
    return seq


if __name__ == "__main__":
    print(fibonacci_sequence(15))
