drop foreign table if exists input;

create foreign table input (raw_data varchar) server aoc2024 options (filename 'input.txt');

with left_side as (
    select
        row_number() over (
            order by
                split_part(raw_data, '   ', 1) :: int
        ) row_id,
        split_part(raw_data, '   ', 1) :: int value
    from
        input
),
right_side as (
    select
        row_number() over (
            order by
                split_part(raw_data, '   ', 2) :: int
        ) row_id,
        split_part(raw_data, '   ', 2) :: int value
    from
        input
),
reconciliation as (
    select
        abs(ls.value - rs.value) distance
    from
        left_side ls
        /* assuming same number of rows at both sides */
        inner join right_side rs on ls.row_id = rs.row_id
)
select
    sum(distance) summation
from
    reconciliation;