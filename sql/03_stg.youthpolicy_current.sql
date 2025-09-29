create table stg.youthpolicy_current
(
    policy_id     text                                   not null
        primary key,
    record_hash   char(64)                               not null,
    first_seen_at timestamp with time zone default now() not null,
    last_seen_at  timestamp with time zone default now() not null,
    is_active     boolean                  default true  not null
);

alter table stg.youthpolicy_current
    owner to admin;

create index idx_stg_current_hash
    on stg.youthpolicy_current (record_hash);

