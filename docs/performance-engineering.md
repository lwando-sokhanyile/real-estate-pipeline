# Performance Engineering

## Historical Dataset

The pipeline processes the full HM Land Registry Price Paid dataset.

- Rows: 31,610,742
- Historical coverage: January 1995 to August 2026
- PostgreSQL target: staging.price_paid

## Baseline

The benchmark calculated monthly county transaction count, average sale price, and exact median sale price using percentile_cont(0.5).

Baseline execution time:

290.95 seconds (~4 minutes 51 seconds)

The execution plan showed:

- Parallel sequential scan across the full 31.6M-row table
- External merge sorting
- Temporary disk usage
- GroupAggregate
- Exact percentile calculation

## Index Experiment

An index was tested on:

(transfer_year, transfer_month, county)

The normal PostgreSQL planner continued to prefer a sequential scan.

When index usage was forced, execution time was:

269.30 seconds

The index still required processing all 31.6M rows and did not eliminate the expensive aggregation work.

Conclusion:

The index was not retained.

## Aggregation Experiment

The median calculation was temporarily removed.

COUNT + AVG execution time:

81.13 seconds

This demonstrated that the exact median calculation was responsible for the majority of the query cost.

## work_mem Experiment

The aggregation was tested with:

SET work_mem = '256MB';

COUNT + AVG execution time:

53.43 seconds

The previous hash aggregation spilled to disk in multiple batches. With 256MB work memory, the aggregation completed in a single batch.

The full COUNT + AVG + exact median query with 256MB work_mem completed in:

198.76 seconds

The exact median still caused an external merge sort with approximately 1.1GB of temporary disk usage.

## Findings

The investigation identified four important performance characteristics:

1. Full-history aggregation requires scanning the 31.6M-row transaction table.
2. Exact median calculation is the dominant computational bottleneck.
3. Additional work memory can reduce aggregation spill, but does not eliminate the expensive median sort.
4. The tested composite index did not provide a meaningful improvement for the full-history aggregation.

## Engineering Direction

The current dbt models remain functionally correct and all existing data-quality tests pass.

Rather than adding indexes without evidence, future optimization should focus on reducing repeated scans of the full historical transaction table.

A potential architecture is to introduce pre-aggregated monthly county statistics and build downstream analytical marts from those aggregates.

Any optimization will be benchmarked against the baseline before being adopted.
