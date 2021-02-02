
"""
Tests:

    Empty query, invoker-set
    Empty query, worker-set, local
        Check publish set size, and that it's depleted at end
    Empty query, worker-set, S3
    Empty query, worker-set, S3 - again
        All from cache

    Build retry task
        is always worker-set, completes as planned as attempt no. 1

    Number and map aggregations results
        Trimmed size of results
    Funnel results
        with aggrs

    Reduce tests:
        Numbers with 0
        Maps
        All query/aggregation levels
        Nested shite

TODO retry mech. - where?
"""