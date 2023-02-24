# `::dbgroup::random`

## `ZipfDistribution<IntType>`

This class generates random values according to Zipf's law. This class can simulate exact the Zipf distribution, but it may take some time to construct an accurate cumulative distribution function.

## `ApproxZipfDistribution<IntType>`

This class approximates the Zipf distribution by using Chlebus's approximate formula for a partial sum of the divergent p-serires[^1]. Note that this approximation works well when the following conditions are satisfied:

1. a distribution has sufficient bins (100,000 or more recommended) and
2. a skew parameter is in [0, 1].

Thus, we compute exact CDFs with $k < 100$ and use them for corresponding queries. If CDF values with $k \geq 100$ are required, we use Chlebus's approximate formula.

The following figure shows the comparison between approximate and exact CDFs.

<img src="figures/comparison_exact_approx_zipf.svg" width="540px">

[^1]: [Edward Chlebus, "An approximate formula for a partial sum of the divergent p-series," Applied Mathematics Letters, Vol. 22, No. 5, pp. 732-737, 2009.](https://doi.org/10.1016/j.aml.2008.07.007)
