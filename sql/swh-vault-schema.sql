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

create type cook_status as enum ('new', 'pending', 'done');
comment on type cook_status is 'Status of the cooking';

create table cook_requests (
  id bigserial primary key,
  type text not null,
  object_id obj_hash not null,
  status cook_status not null
);

create table cook_notifications (
  id bigserial primary key,
  email text not null,
  request_id bigint not null references cook_requests(id)
);
