-- SWH DB schema upgrade
-- from_version: 001
-- to_version: 002
-- description: Add batches and various indexes

insert into dbversion(version, release, description)
    values(2, now(), 'Add batches and various indexes');

create unique index concurrently if not exists vault_bundle_type_object
  on vault_bundle (type, object_id);
create index concurrently if not exists vault_bundle_task_id
  on vault_bundle (task_id);

create index concurrently if not exists vault_notif_email_bundle
  on vault_notif_email (bundle_id);
create index concurrently if not exists vault_notif_email_email
  on vault_notif_email (email);

create table if not exists vault_batch (
  id bigserial primary key
);

create table if not exists vault_batch_bundle (
  batch_id bigint not null references vault_batch(id),
  bundle_id bigint not null references vault_bundle(id)
);
create unique index concurrently if not exists vault_batch_bundle_pkey
  on vault_batch_bundle (batch_id, bundle_id);
