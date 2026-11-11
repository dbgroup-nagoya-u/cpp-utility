# `::dbgroup::random`

- [class ZipfDistribution](#class-zipfdistribution)
- [class ApproxZipfDistribution](#class-approxzipfdistribution)
    - [Example of Usages](#example-of-usages)

## class ZipfDistribution

This class generates random values according to Zipf's law. This class can accurately simulate the Zipf distribution, but it may take some time to construct an accurate cumulative distribution function.

Note that a template `IntType` must be 32/64-bit integers, such as `size_t` and `int64_t`. The constructor throws `std::runtime_error` if the maximum value is less than the minimum one or the range covers all the 64-bit integers.

## class ApproxZipfDistribution

This class approximates the Zipf distribution in constant time and space regardless of the number of bins. We compute exact CDFs for the first 100 bins and use them for both CDF queries and random generation. For the remaining bins, we approximate the partial sum of $k^{-\alpha}$ by integrating $x^{-\alpha}$ with the midpoint rule, which also gives a closed-form inverse for random generation. In our measurements with $\alpha \in [0, 3]$ and up to $10^7$ bins, the maximum absolute error of approximate CDFs was less than $3 \times 10^{-7}$.

Note that CDFs are represented by double-precision floating-point numbers, so adjacent bins may not be distinguished when a distribution has an extremely large number of bins (e.g., $10^{12}$ or more).

Note that a template `IntType` must be 32/64-bit integers, such as `size_t` and `int64_t`, and the same exceptions as `ZipfDistribution` are thrown for invalid ranges.

### Example of Usages

```cpp
// C++ standard libraries
#include <iostream>

// our libraries
#include "dbgroup/random/zipf.hpp"

auto
main(  //
    const int argc,
    const char *argv[])  //
    -> int
{
  constexpr size_t kMin = 0;
  constexpr size_t kMax = 1000;
  constexpr double skew = 1.0;

  // create a Zipf distribution and a random number generator
  ::dbgroup::random::ApproxZipfDistribution zipf{kMin, kMax, skew};
  std::mt19937_64 rand_engine{std::random_device{}()};

  // get samples according to the Zipf distribution
  for (size_t i = 0; i < 30; ++i) {
    std::cout << zipf(rand_engine) << std::endl;
  }

  return 0;
}
```

The above example outputs thirty random values according to the Zipf distribution as follows.

```txt
45
430
2
36
2
55
11
108
201
462
42
45
100
73
0
49
10
1
0
509
0
0
90
11
32
10
36
208
6
0
```
