-- auto-generated definition
create table funds
(
    id                       serial
        primary key,
    code                     varchar(1000) not null,
    name                     varchar(1000) not null,
    trustee                  varchar(1000) not null,
    operate_mode_id          integer       not null,
    operate_mode             varchar(1000) not null,
    start_date               date,
    end_date                 date,
    advisor                  varchar(2000),
    total_tna                bigint,
    net_value                double precision,
    quote_change_weekly      double precision,
    quote_change_monthly     double precision,
    underlying_asset_type    varchar(100),
    underlying_asset_type_id integer
);

alter table funds
    owner to zillionare;

create unique index funds_code_uindex
    on funds (code);

-- auto-generated definition
create table fund_net_value
(
    id                 serial
        primary key,
    code               varchar(1000)    not null,
    net_value          double precision not null,
    sum_value          double precision not null,
    factor             double precision not null,
    acc_factor         double precision not null,
    refactor_net_value double precision not null,
    day                date             not null,
    shares             bigint
);

alter table fund_net_value
    owner to zillionare;

create unique index fund_net_value_code_day_uindex
    on fund_net_value (code, day);

-- auto-generated definition
create table fund_portfolio_stock
(
    id             serial
        primary key,
    code           varchar(1000)    not null,
    period_start   date             not null,
    period_end     date             not null,
    pub_date       date             not null,
    report_type_id integer          not null,
    report_type    varchar(1000)    not null,
    rank           integer          not null,
    symbol         varchar(1000)    not null,
    name           varchar(1000)    not null,
    shares         double precision not null,
    market_cap     double precision not null,
    proportion     double precision not null,
    deadline       date
);

alter table fund_portfolio_stock
    owner to zillionare;

create unique index fund_portfolio_stock_code_pub_date_symbol_report_type_uindex
    on fund_portfolio_stock (code, pub_date, symbol, report_type);

-- auto-generated definition
create table fund_share_daily
(
    id        serial
        primary key,
    code      varchar(1000) not null,
    name      varchar(1000) not null,
    date      date          not null,
    total_tna double precision
);

alter table fund_share_daily
    owner to zillionare;

create unique index fund_share_daily_code_date_uindex
    on fund_share_daily (code, date);
