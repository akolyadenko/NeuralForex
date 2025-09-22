drop table if exists m1_data;
create table m1_data as select * from eurusd_data;

alter table m1_data add epoch_min bigint;

update m1_data set epoch_min = extract(epoch from make_timestamp(
        year::int, month::int, day::int, hour::int, minute::int, 0)) / 60;

drop table if exists timepoint;
create table timepoint as
select
    epoch_min,
    year,
    month,
    day,
    hour,
    minute,
    make_timestamp(year::int, month::int, day::int, hour::int,
                   minute::int, 0) "time"
from m1_data;

drop table if exists timeframe;
create table timeframe as
select
    'm1' "type",
    epoch_min,
    open,
    high,
    low,
    close,
    volume
from m1_data;

insert into timeframe
select
        'm5' "type",
        5 * (epoch_min / 5) epoch_min,
        (array_agg(open order by epoch_min)::double precision[])[1] open,
        max(high) high,
        min(low) low,
        (array_agg(close order by epoch_min desc)::double precision[])[1] "close",
        sum(volume) volume
from m1_data
group by 1, 2;

insert into timeframe
select
    'm15' "type",
    15 * (epoch_min / 15) epoch_min,
    (array_agg(open order by epoch_min)::double precision[])[1] open,
    max(high) high,
    min(low) low,
    (array_agg(close order by epoch_min desc)::double precision[])[1] "close",
    sum(volume) volume
from m1_data
group by 1, 2;

insert into timeframe
select
    'm30' "type",
    30 * (epoch_min / 30) epoch_min,
    (array_agg(open order by epoch_min)::double precision[])[1] open,
    max(high) high,
    min(low) low,
    (array_agg(close order by epoch_min desc)::double precision[])[1] "close",
    sum(volume) volume
from m1_data
group by 1, 2;

insert into timeframe
select
    'h1' "type",
    60 * (epoch_min / 60) epoch_min,
    (array_agg(open order by epoch_min)::double precision[])[1] open,
    max(high) high,
    min(low) low,
    (array_agg(close order by epoch_min desc)::double precision[])[1] "close",
    sum(volume) volume
from m1_data
group by 1, 2;

insert into timeframe
select
    'h4' "type",
    240 * (epoch_min / 240) epoch_min,
    (array_agg(open order by epoch_min)::double precision[])[1] open,
    max(high) high,
    min(low) low,
    (array_agg(close order by epoch_min desc)::double precision[])[1] "close",
    sum(volume) volume
from m1_data
group by 1, 2;

create index timeframe_type_epoch_min_idx on timeframe(type, epoch_min);

drop table if exists example;
create table example as
select
    epoch_min
from timepoint
order by random()
-- limit 100000
;

alter table example add split text;

update example
set split = 'train'
where epoch_min < extract(epoch from make_timestamp(
        2022, 03, 01, 00, 0, 0)) / 60;

update example
set split = 'test'
where epoch_min >= extract(epoch from make_timestamp(
        2022, 02, 01, 00, 0, 0)) / 60
    and epoch_min < extract(epoch from make_timestamp(
        2022, 04, 01, 00, 0, 0)) / 60;;

update example
set split = 'eval'
where epoch_min >= extract(epoch from make_timestamp(
        2022, 04, 01, 00, 0, 0)) / 60;