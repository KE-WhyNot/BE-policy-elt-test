create table stg.youthpolicy_landing
(
    policy_id     text                                   not null,
    record_hash   char(64)                               not null,
    raw_json      jsonb                                  not null,
    ingested_at   timestamp with time zone default now() not null,
    raw_ingest_id uuid                                   not null,
    page_no       integer                                not null,
    primary key (policy_id, record_hash)
);

alter table stg.youthpolicy_landing
    owner to admin;

create index idx_stg_landing_raw
    on stg.youthpolicy_landing (raw_ingest_id);

create index idx_stg_landing_policy
    on stg.youthpolicy_landing (policy_id);

