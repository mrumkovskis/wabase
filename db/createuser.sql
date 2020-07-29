-- sudo -u postgres psql postgres
create user core with password 'core';
alter user core with superuser;
create database "core" encoding 'utf8' owner core;
\c core
alter schema public owner to core;
