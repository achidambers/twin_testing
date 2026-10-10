import unittest

from fibonacci import fibonacci, fibonacci_sequence


class TestFibonacci(unittest.TestCase):
    def test_base_cases(self):
        self.assertEqual(fibonacci(0), 0)
        self.assertEqual(fibonacci(1), 1)

    def test_small_values(self):
        expected = [0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55]
        for n, value in enumerate(expected):
            with self.subTest(n=n):
                self.assertEqual(fibonacci(n), value)

    def test_larger_values(self):
        self.assertEqual(fibonacci(30), 832040)
        self.assertEqual(fibonacci(50), 12586269025)

    def test_big_int(self):
        self.assertEqual(fibonacci(100), 354224848179261915075)

    def test_recurrence_holds(self):
        for n in range(2, 200):
            with self.subTest(n=n):
                self.assertEqual(fibonacci(n), fibonacci(n - 1) + fibonacci(n - 2))

    def test_negative_raises(self):
        with self.assertRaises(ValueError):
            fibonacci(-1)

    def test_non_int_raises(self):
        for bad in (1.5, "5", None, True):
            with self.subTest(bad=bad):
                with self.assertRaises(TypeError):
                    fibonacci(bad)


class TestFibonacciSequence(unittest.TestCase):
    def test_empty(self):
        self.assertEqual(fibonacci_sequence(0), [])

    def test_first_ten(self):
        self.assertEqual(fibonacci_sequence(10), [0, 1, 1, 2, 3, 5, 8, 13, 21, 34])

    def test_matches_fibonacci(self):
        seq = fibonacci_sequence(50)
        self.assertEqual(seq, [fibonacci(i) for i in range(50)])

    def test_negative_raises(self):
        with self.assertRaises(ValueError):
            fibonacci_sequence(-3)

    def test_non_int_raises(self):
        with self.assertRaises(TypeError):
            fibonacci_sequence(2.0)


if __name__ == "__main__":
    unittest.main()
