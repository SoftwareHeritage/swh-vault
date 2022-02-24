create domain obj_hash as bytea;

create type bundle_type as enum ('flat', 'gitfast', 'git_bare');
comment on type bundle_type is 'Type of the requested bundle';

create type cook_status as enum ('new', 'pending', 'done', 'failed');
comment on type cook_status is 'Status of the cooking';

create table vault_bundle (
  id bigserial primary key,

  type bundle_type not null,
  swhid text not null,  -- requested object ID

  task_id integer,  -- scheduler task id
  task_status cook_status not null default 'new',  -- status of the task
  sticky boolean not null default false, -- bundle cannot expire

  ts_created timestamptz not null default now(),  -- timestamp of creation
  ts_done timestamptz,  -- timestamp of the cooking result
  ts_last_access timestamptz not null default now(),  -- last access

  progress_msg text -- progress message
);
create unique index concurrently vault_bundle_type_swhid
  on vault_bundle (type, swhid);
create index concurrently vault_bundle_task_id
  on vault_bundle (task_id);

create table vault_notif_email (
  id bigserial primary key,
  email text not null,              -- e-mail to notify
  bundle_id bigint not null references vault_bundle(id) on delete cascade
);
create index concurrently vault_notif_email_bundle
  on vault_notif_email (bundle_id);
create index concurrently vault_notif_email_email
  on vault_notif_email (email);

create table vault_batch (
  id bigserial primary key
);

create table vault_batch_bundle (
  batch_id bigint not null references vault_batch(id) on delete cascade,
  bundle_id bigint not null references vault_bundle(id) on delete cascade
);
create unique index concurrently vault_batch_bundle_pkey
  on vault_batch_bundle (batch_id, bundle_id);
