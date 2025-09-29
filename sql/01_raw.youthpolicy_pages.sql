create table raw.youthpolicy_pages
(
    ingest_id    uuid                     default gen_random_uuid() not null
        primary key,
    ingested_at  timestamp with time zone default now()             not null,
    page_no      integer                                            not null,
    page_size    integer,
    base_url     text                                               not null,
    http_status  integer                                            not null,
    query_params jsonb                                              not null,
    payload      jsonb                                              not null
);

alter table raw.youthpolicy_pages
    owner to admin;

create index idx_raw_yp_pages_time
    on raw.youthpolicy_pages (ingested_at desc);

create index idx_raw_yp_pages_page
    on raw.youthpolicy_pages (page_no);

