set time zone UTC;

drop table if exists oanda_candle_copy;
create table oanda_candle_copy as select * from oanda_candle;

alter table oanda_candle_copy add time_parsed timestamptz;

update oanda_candle_copy set time = replace(time, '.000000000Z', '');
update oanda_candle_copy set time = replace(time, 'T', '-');
update oanda_candle_copy set time_parsed = to_timestamp(time, 'YYYY-MM-DD-HH24:MI:SS') at time zone 'Etc/UTC';

alter table oanda_candle_copy add epoch_min bigint;
update oanda_candle_copy set epoch_min = extract(epoch from time_parsed) / 60;

alter table oanda_candle_copy add id bigint default nextval('ids');

with grouped_by as (
    select
        epoch_min,
        type,
        max(id) as id
    from oanda_candle_copy
    group by 1, 2
), ids as (
    select distinct id from grouped_by
)
delete from oanda_candle_copy where id not in (select id from ids);

create unique index oanda_candle_pk on oanda_candle_copy(epoch_min, type);

drop table if exists timeframe;
create table timeframe as
select
    type,
    epoch_min,
    open,
    high,
    low,
    close,
    volume,
    time_parsed as time,
    false as generated
from oanda_candle_copy;

drop table if exists epoch_min_range;
create temp table epoch_min_range as
select
    min(epoch_min) epoch_min_min,
    max(epoch_min) epoch_min_max
from timeframe;

drop table if exists epoch_mins;
create table epoch_mins as
select generate_series(epoch_min_min, epoch_min_max) epoch_min
from epoch_min_range;

drop table if exists timeframe_missing;
create table timeframe_missing as
select
    epoch_mins.epoch_min
from epoch_mins
left join timeframe tmf2 on tmf2.epoch_min = epoch_mins.epoch_min
where tmf2.epoch_min is null
group by 1;

create unique index timeframe_type_epoch_min_idx on timeframe(type, epoch_min);
create index timeframe_epoch_min_idx on timeframe(epoch_min);

drop table if exists timeframe_missing_2;
create table timeframe_missing_2 as
select
    tfm1.epoch_min,
    max(tmf2.epoch_min) as prev_epoch_min
from timeframe_missing tfm1
join timeframe tmf2 on tmf2.epoch_min < tfm1.epoch_min
    and tmf2.epoch_min > tfm1.epoch_min - 60 * 24 * 7
group by 1;

insert into timeframe (
    type,
    epoch_min,
    open,
    high,
    low,
    close,
    volume,
    time,
    generated
)
select
    t.type,
    m.epoch_min,
    t.open,
    t.high,
    t.low,
    t.close,
    volume,
    to_timestamp(m.epoch_min * 60),
    true as generated
from timeframe_missing_2 m
join timeframe t on t.epoch_min = m.prev_epoch_min;

drop table if exists timepoint;
create table timepoint as
select distinct
    epoch_min,
    time
from timeframe;

alter table timepoint add year bigint;
alter table timepoint add month bigint;
alter table timepoint add day bigint;
alter table timepoint add hour bigint;
alter table timepoint add minute bigint;

update timepoint set year = extract(year from time);
update timepoint set month = extract(month from time);
update timepoint set day = extract(day from time);
update timepoint set hour = extract(hour from time);
update timepoint set minute = extract(minute from time);

create unique index on timepoint(epoch_min);

drop table if exists example;

create table example as
select
    epoch_min,
    time
from timeframe
where type = 'ask'
order by random()
-- limit 100000
;

alter table example add split text;

update example
set split = 'train';

update example
set split = 'test'
where time >= to_timestamp('2022-01-01', 'YYYY-MM-DD')
    and time < to_timestamp('2022-06-01', 'YYYY-MM-DD');

update example
set split = 'eval1'
where time >= to_timestamp('2022-06-01', 'YYYY-MM-DD')
  and time < to_timestamp('2022-07-01', 'YYYY-MM-DD');

update example
set split = 'eval2'
where time >= to_timestamp('2022-07-01', 'YYYY-MM-DD')
  and time < to_timestamp('2022-08-01', 'YYYY-MM-DD');

update example
set split = 'eval3'
where time >= to_timestamp('2022-08-01', 'YYYY-MM-DD')
  and time < to_timestamp('2022-09-01', 'YYYY-MM-DD');

-- update example set split = 'eval'
-- where (epoch_min / (24 * 60 * 7)) % 13 in (4);

-- update example set split = 'test'
-- where (epoch_min / (24 * 60 * 7)) % 13 in (0, 1, 2, 3);

-- update example
-- set split = 'train'
-- where time < to_timestamp('2022-03-01', 'YYYY-MM-DD');