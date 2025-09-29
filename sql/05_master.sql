create table region
(
    id         bigserial
        primary key,
    code       varchar(32)                                       not null,
    name       varchar(64)                                       not null,
    parent_id  bigint
        references region,
    kind       varchar(16) default 'PROVINCE'::character varying not null,
    is_active  boolean     default true                          not null,
    created_at timestamp   default now()                         not null,
    updated_at timestamp   default now()                         not null,
    zip_code   text,
    full_name  text
);

alter table region
    owner to admin;

create unique index uq_region_code
    on region (code);

create table category
(
    id         bigserial
        primary key,
    code       varchar(32)                                    not null,
    name       varchar(100)                                   not null,
    parent_id  bigint
        references category,
    level      varchar(16) default 'LARGE'::character varying not null,
    is_active  boolean     default true                       not null,
    created_at timestamp   default now()                      not null,
    updated_at timestamp   default now()                      not null
);

alter table category
    owner to admin;

create unique index uq_category_code
    on category (code);

create table keyword
(
    id         bigserial
        primary key,
    name       varchar(64)             not null
        unique,
    is_active  boolean   default true  not null,
    created_at timestamp default now() not null,
    updated_at timestamp default now() not null
);

alter table keyword
    owner to admin;

create table education
(
    id         bigserial
        primary key,
    name       varchar(32)             not null
        unique,
    is_active  boolean   default true  not null,
    created_at timestamp default now() not null,
    updated_at timestamp default now() not null,
    code       varchar(16)
        constraint uq_master_education_code
            unique
);

alter table education
    owner to admin;

create table major
(
    id         bigserial
        primary key,
    name       varchar(64)             not null
        unique,
    is_active  boolean   default true  not null,
    created_at timestamp default now() not null,
    updated_at timestamp default now() not null,
    code       varchar(16)
        constraint uq_master_major_code
            unique
);

alter table major
    owner to admin;

create table job_status
(
    id         bigserial
        primary key,
    name       varchar(32)             not null
        unique,
    is_active  boolean   default true  not null,
    created_at timestamp default now() not null,
    updated_at timestamp default now() not null,
    code       varchar(16)
        constraint uq_master_job_status_code
            unique
);

alter table job_status
    owner to admin;

create table specialization
(
    id         bigserial
        primary key,
    name       varchar(64)             not null
        unique,
    is_active  boolean   default true  not null,
    created_at timestamp default now() not null,
    updated_at timestamp default now() not null,
    code       varchar(16)
        constraint uq_master_specialization_code
            unique
);

alter table specialization
    owner to admin;

