-- SWH DB schema upgrade
-- from_version: 002
-- to_version: 003
-- description: Add delete cascade conditions

insert into dbversion(version, release, description)
    values(3, now(), 'Add delete cascade conditions');

alter table vault_notif_email
    drop constraint "vault_notif_email_bundle_id_fkey",
    add constraint "vault_notif_email_bundle_id_fkey"
    foreign key(bundle_id)
    references vault_bundle
    on delete cascade;

alter table vault_batch_bundle
    drop constraint "vault_batch_bundle_bundle_id_fkey",
    add constraint "vault_batch_bundle_bundle_id_fkey"
    foreign key(bundle_id)
    references vault_bundle
    on delete cascade;

alter table vault_batch_bundle
    drop constraint "vault_batch_bundle_batch_id_fkey",
    add constraint "vault_batch_bundle_batch_id_fkey"
    foreign key(batch_id)
    references vault_batch
    on delete cascade;
