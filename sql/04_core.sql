create sequence policy_id_seq;

alter sequence policy_id_seq owner to admin;

create table policy
(
    id                     text                     default nextval('core.policy_id_seq'::regclass) not null
        primary key,
    ext_source             varchar(64),
    ext_id                 varchar(32),
    title                  varchar(255),
    summary_raw            text,
    description_raw        text,
    summary_ai             text,
    status                 text                     default 'UNKNOWN'::text,
    apply_start            date,
    apply_end              date,
    last_external_modified timestamp with time zone,
    views                  integer                  default 0,
    supervising_org        varchar(100),
    operating_org          varchar(100),
    apply_url              text,
    ref_url_1              text,
    ref_url_2              text,
    created_at             timestamp with time zone default now(),
    updated_at             timestamp with time zone default now(),
    payload                jsonb,
    content_hash           text,
    period_start           date,
    period_etc             text,
    period_end             date,
    apply_type             text,
    period_type            text,
    announcement           text,
    info_etc               text,
    first_external_created timestamp with time zone,
    application_process    text,
    required_documents     text,
    constraint ux_policy_source_extid
        unique (ext_source, ext_id)
);

alter table policy
    owner to admin;

alter sequence policy_id_seq owned by policy.id;

create index ix_policy_apply_dates
    on policy (apply_start, apply_end);

create index ix_policy_last_external_modified
    on policy (last_external_modified);

create index ix_policy_created_at
    on policy (created_at);

create index ft_policy_text
    on policy using gin (to_tsvector('simple'::regconfig,
                                     (((((title::text || ' '::text) || summary_raw) || ' '::text) || description_raw) ||
                                      ' '::text) || summary_ai));

create index ix_policy_status
    on policy (status);

create table policy_eligibility
(
    policy_id               text                                   not null
        primary key
        constraint fk_policy_eligibility_policy
            references policy
            on delete cascade,
    marital_status          text                                   not null,
    age_min                 integer,
    age_max                 integer,
    income_type             text                                   not null,
    income_min              integer,
    income_max              integer,
    income_text             text,
    created_at              timestamp with time zone default now() not null,
    updated_at              timestamp with time zone default now() not null,
    eligibility_additional  text,
    eligibility_restrictive integer,
    restrict_education      boolean,
    restrict_major          boolean,
    restrict_job_status     boolean,
    restrict_specialization boolean
);

alter table policy_eligibility
    owner to admin;

create table policy_region
(
    policy_id  text                                   not null
        constraint fk_pr_policy
            references policy
            on delete cascade,
    region_id  bigint                                 not null
        constraint fk_pr_region
            references master.region
            on delete cascade,
    created_at timestamp with time zone default now() not null,
    updated_at timestamp with time zone default now() not null,
    primary key (policy_id, region_id)
);

alter table policy_region
    owner to admin;

create index idx_policy_region_region
    on policy_region (region_id);

create index idx_policy_region_policy
    on policy_region (policy_id);

create table policy_category
(
    policy_id   text                                   not null
        constraint fk_pc_policy
            references policy
            on delete cascade,
    category_id bigint                                 not null
        constraint fk_pc_category
            references master.category
            on delete cascade,
    created_at  timestamp with time zone default now() not null,
    updated_at  timestamp with time zone default now() not null,
    primary key (policy_id, category_id)
);

alter table policy_category
    owner to admin;

create table policy_keyword
(
    policy_id  text                                   not null
        constraint fk_pk_policy
            references policy
            on delete cascade,
    keyword_id bigint                                 not null
        constraint fk_pk_keyword
            references master.keyword
            on delete cascade,
    created_at timestamp with time zone default now() not null,
    updated_at timestamp with time zone default now() not null,
    primary key (policy_id, keyword_id)
);

alter table policy_keyword
    owner to admin;

create table policy_eligibility_education
(
    policy_id    text                                   not null
        constraint fk_pee_policy
            references policy
            on delete cascade,
    education_id bigint                                 not null
        constraint fk_pee_education
            references master.education
            on delete cascade,
    created_at   timestamp with time zone default now() not null,
    updated_at   timestamp with time zone default now() not null,
    primary key (policy_id, education_id)
);

alter table policy_eligibility_education
    owner to admin;

create table policy_eligibility_major
(
    policy_id  text                                   not null
        constraint fk_pem_policy
            references policy
            on delete cascade,
    major_id   bigint                                 not null
        constraint fk_pem_major
            references master.major
            on delete cascade,
    created_at timestamp with time zone default now() not null,
    updated_at timestamp with time zone default now() not null,
    primary key (policy_id, major_id)
);

alter table policy_eligibility_major
    owner to admin;

create table policy_eligibility_job_status
(
    policy_id     text                                   not null
        constraint fk_pejs_policy
            references policy
            on delete cascade,
    job_status_id bigint                                 not null
        constraint fk_pejs_jobstatus
            references master.job_status
            on delete cascade,
    created_at    timestamp with time zone default now() not null,
    updated_at    timestamp with time zone default now() not null,
    primary key (policy_id, job_status_id)
);

alter table policy_eligibility_job_status
    owner to admin;

create table policy_eligibility_specialization
(
    policy_id         text                                   not null
        constraint fk_pes_policy
            references policy
            on delete cascade,
    specialization_id bigint                                 not null
        constraint fk_pes_specialization
            references master.specialization
            on delete cascade,
    created_at        timestamp with time zone default now() not null,
    updated_at        timestamp with time zone default now() not null,
    primary key (policy_id, specialization_id)
);

alter table policy_eligibility_specialization
    owner to admin;

create function touch_updated_at() returns trigger
    language plpgsql
as
$$
BEGIN NEW.updated_at := now(); RETURN NEW; END$$;

alter function touch_updated_at() owner to admin;