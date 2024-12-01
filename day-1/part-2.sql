drop foreign table if exists input;

create foreign table input (raw_data varchar) server aoc2024 options (filename 'input.txt');

with left_side as (
    select
        split_part(raw_data, '   ', 1) :: int value
    from
        input
),
right_side as (
    select
        split_part(raw_data, '   ', 2) :: int value
    from
        input
),
reconciliation as (
    /*
     for each item on ls, count frequency at rs.
     if item on ls appars multiple times, group into one
     */
    select
        ls.value * count(*) similarity
    from
        left_side ls
        /* assuming same number of rows in both inputs */
        inner join right_side rs on ls.value = rs.value
    group by
        ls.value
)
select
    sum(similarity) score
from
    reconciliation;