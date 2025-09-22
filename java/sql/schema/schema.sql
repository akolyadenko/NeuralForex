create table strategy_trade (
    strategy text,
    trade_id text
);

create table trader_tick_data (
    data jsonb
);

alter table trader_tick_data add m1close_rescaled real;
alter table trader_tick_data add m1close real;