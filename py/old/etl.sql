alter table xauusd_data add epoch_min bigint;

update xauusd_data set epoch_min = extract(epoch from make_timestamp(
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
from xauusd_data;

drop table if exists xau_m1;
create table xau_m1 as
select
       epoch_min,
       open,
       high,
       low,
       close,
       volume
from xauusd_data;

drop table if exists xau_m5;
create table xau_m5 as
select
    5 * (epoch_min / 5) epoch_min,
    (array_agg(open order by epoch_min)::double precision[])[1] open,
    max(high) high,
    min(low) low,
    (array_agg(close order by epoch_min desc)::double precision[])[1] "close",
    sum(volume) volume
from xau_m1
group by 1;

drop table if exists xau_m15;
create table xau_m15 as
select
        15 * (epoch_min / 15) epoch_min,
        (array_agg(open order by epoch_min)::double precision[])[1] open,
        max(high) high,
        min(low) low,
        (array_agg(close order by epoch_min desc)::double precision[])[1] "close",
        sum(volume) volume
from xau_m1
group by 1;

drop table if exists xau_m30;
create table xau_m30 as
select
        30 * (epoch_min / 30) epoch_min,
        (array_agg(open order by epoch_min)::double precision[])[1] open,
        max(high) high,
        min(low) low,
        (array_agg(close order by epoch_min desc)::double precision[])[1] "close",
        sum(volume) volume
from xau_m1
group by 1;

drop table if exists xau_h1;
create table xau_h1 as
select
        60 * (epoch_min / 60) epoch_min,
        (array_agg(open order by epoch_min)::double precision[])[1] open,
        max(high) high,
        min(low) low,
        (array_agg(close order by epoch_min desc)::double precision[])[1] "close",
        sum(volume) volume
from xau_m1
group by 1;

drop table if exists xau_h4;
create table xau_h4 as
select
        240 * (epoch_min / 240) epoch_min,
        (array_agg(open order by epoch_min)::double precision[])[1] open,
        max(high) high,
        min(low) low,
        (array_agg(close order by epoch_min desc)::double precision[])[1] "close",
        sum(volume) volume
from xau_m1
group by 1;

drop table if exists example;
create table example as
select
       epoch_min
from timepoint
order by random()
limit 100000;

alter table example add m1_open double precision[];
alter table example add m1_high double precision[];
alter table example add m1_low double precision[];
alter table example add m1_close double precision[];
alter table example add m1_volume double precision[];

update example set
                   m1_open = open,
                   m1_high = high,
                   m1_low = low,
                   m1_close = close,
                   m1_volume = volume
from (
    select
        e.epoch_min,
        array_agg(open order by m.epoch_min) open,
        array_agg(high order by m.epoch_min) high,
        array_agg(low order by m.epoch_min) low,
        array_agg(close order by m.epoch_min) "close",
        array_agg(volume order by m.epoch_min) volume
    from example e
    join xau_m1 m on
        m.epoch_min <= e.epoch_min
        and m.epoch_min > e.epoch_min - 60
    group by 1
) d
where d.epoch_min = example.epoch_min;

alter table example add m5_open double precision[];
alter table example add m5_high double precision[];
alter table example add m5_low double precision[];
alter table example add m5_close double precision[];
alter table example add m5_volume double precision[];

update example set
                   m5_open = open,
                   m5_high = high,
                   m5_low = low,
                   m5_close = close,
                   m5_volume = volume
from (
         select
             e.epoch_min,
             array_agg(open order by m.epoch_min) open,
             array_agg(high order by m.epoch_min) high,
             array_agg(low order by m.epoch_min) low,
             array_agg(close order by m.epoch_min) "close",
             array_agg(volume order by m.epoch_min) volume
         from example e
                  join xau_m5 m on
                     m.epoch_min <= e.epoch_min
                 and m.epoch_min > e.epoch_min - 60 * 5
         group by 1
     ) d
where d.epoch_min = example.epoch_min;

alter table example add m15_open double precision[];
alter table example add m15_high double precision[];
alter table example add m15_low double precision[];
alter table example add m15_close double precision[];
alter table example add m15_volume double precision[];

update example set
                   m15_open = open,
                   m15_high = high,
                   m15_low = low,
                   m15_close = close,
                   m15_volume = volume
from (
         select
             e.epoch_min,
             array_agg(open order by m.epoch_min) open,
             array_agg(high order by m.epoch_min) high,
             array_agg(low order by m.epoch_min) low,
             array_agg(close order by m.epoch_min) "close",
             array_agg(volume order by m.epoch_min) volume
         from example e
                  join xau_m15 m on
                     m.epoch_min <= e.epoch_min
                 and m.epoch_min > e.epoch_min - 60 * 15
         group by 1
     ) d
where d.epoch_min = example.epoch_min;

alter table example add m30_open double precision[];
alter table example add m30_high double precision[];
alter table example add m30_low double precision[];
alter table example add m30_close double precision[];
alter table example add m30_volume double precision[];

update example set
                   m30_open = open,
                   m30_high = high,
                   m30_low = low,
                   m30_close = close,
                   m30_volume = volume
from (
         select
             e.epoch_min,
             array_agg(open order by m.epoch_min) open,
             array_agg(high order by m.epoch_min) high,
             array_agg(low order by m.epoch_min) low,
             array_agg(close order by m.epoch_min) "close",
             array_agg(volume order by m.epoch_min) volume
         from example e
                  join xau_m30 m on
                     m.epoch_min <= e.epoch_min
                 and m.epoch_min > e.epoch_min - 60 * 30
         group by 1
     ) d
where d.epoch_min = example.epoch_min;

alter table example add h1_open double precision[];
alter table example add h1_high double precision[];
alter table example add h1_low double precision[];
alter table example add h1_close double precision[];
alter table example add h1_volume double precision[];

update example set
                   m30_open = open,
                   m30_high = high,
                   m30_low = low,
                   m30_close = close,
                   m30_volume = volume
from (
         select
             e.epoch_min,
             array_agg(open order by m.epoch_min) open,
             array_agg(high order by m.epoch_min) high,
             array_agg(low order by m.epoch_min) low,
             array_agg(close order by m.epoch_min) "close",
             array_agg(volume order by m.epoch_min) volume
         from example e
                  join xau_h1 m on
                     m.epoch_min <= e.epoch_min
                 and m.epoch_min > e.epoch_min - 60 * 60
         group by 1
     ) d
where d.epoch_min = example.epoch_min;