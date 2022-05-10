with two_most_appearing as (
    select region, count(*) count
    from jobsity.trips.trips_staging_table
    group by region
    limit 2
), region_sorting as (
    select region, datasource, row_number() over (partition by region order by datetime desc) rownum
    from jobsity.trips.trips_staging_table
    where region in (select region from two_most_appearing)
    order by datetime desc
)
select region, datasource
from region_sorting
where rownum = 1