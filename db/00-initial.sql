
---- reset db
drop schema public cascade;
create schema public;

-- enables '%~~~%' operator
create extension if not exists unaccent;

-- global id-sequence
create sequence seq;

create or replace function f_unaccent(text)
  returns text as
$func$
select unaccent('unaccent', $1)
$func$ language sql immutable set search_path = public, pg_temp;
