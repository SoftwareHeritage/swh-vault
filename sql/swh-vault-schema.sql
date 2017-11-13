create table dbversion
(
  version     int primary key,
  release     timestamptz not null,
  description text not null
);
comment on table dbversion is 'Schema update tracking';
insert into dbversion (version, release, description)
       values (1, now(), 'Initial version');

create domain obj_hash as bytea;

create type cook_type as enum ('directory', 'revision_gitfast');
comment on type cook_type is 'Type of the requested bundle';

create type cook_status as enum ('new', 'pending', 'done', 'failed');
comment on type cook_status is 'Status of the cooking';

create table vault_bundle (
  id bigserial primary key,

  type cook_type not null,  -- requested cooking type
  object_id obj_hash not null,  -- requested object ID

  task_id integer,  -- scheduler task id
  task_status cook_status not null default 'new',  -- status of the task
  sticky boolean not null default false, -- bundle cannot expire

  ts_created timestamptz not null default now(),  -- timestamp of creation
  ts_done timestamptz,  -- timestamp of the cooking result
  ts_last_access timestamptz not null default now(),  -- last access

  progress_msg text, -- progress message

  unique(type, object_id)
);

create table vault_notif_email (
  id bigserial primary key,
  email text not null,              -- e-mail to notify
  bundle_id bigint not null references vault_bundle(id)
);
