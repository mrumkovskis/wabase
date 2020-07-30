create table deferred_request(
  username varchar(50) not null,
  priority integer not null,
  request_time timestamp not null,
  status varchar(5) not null check (status in ('OK', 'ERR', 'QUEUE', 'EXE', 'DEL')),
  topic varchar(50) not null,
  request_hash varchar(100),
  request bytea not null,
  response_time timestamp,
  result bytea,
  result_file_id bigint,
  result_file_sha_256 varchar(64)
);
alter table deferred_request add constraint pk_deferred_request primary key (request_hash);
create index idx_deferred_request_priority_request_time on deferred_request(priority, request_time);
create index idx_deferred_request_username on deferred_request(username);

create table file_body_info(
  sha_256 varchar(64),
  size bigint not null,
  path varchar(240) not null
);
alter table file_body_info add constraint pk_file_body_info primary key (sha_256);

create table file_info(
  id bigint,
  filename varchar(240) not null,
  upload_time timestamp not null,
  content_type varchar(100) not null,
  sha_256 varchar(64) not null
);
alter table file_info add constraint pk_file_info primary key (id);

create table validation(
  id bigint,
  context varchar(100) not null,
  expression varchar(500) not null,
  message varchar(500) not null
);
comment on table validation is 'Configurable data validation with error message';
comment on column validation.context is 'Context - view name';
comment on column validation.expression is 'Expression - javascript logical expression';
comment on column validation.message is 'Message - error message, if expression is false';
alter table validation add constraint pk_validation primary key (id);

alter table file_info add constraint fk_file_info_sha_256 foreign key (sha_256) references file_body_info(sha_256);

create table deferred_file_body_info(
  sha_256 varchar(64),
  size bigint not null,
  path varchar(240) not null
);
alter table deferred_file_body_info add constraint pk_deferred_file_body_info primary key (sha_256);

create table deferred_file_info(
  id bigint,
  filename varchar(240) not null,
  upload_time timestamp not null,
  content_type varchar(100) not null,
  sha_256 varchar(64) not null
);
alter table deferred_file_info add constraint pk_deferred_file_info primary key (id);
alter table deferred_file_info add constraint fk_deferred_file_info_sha_256 foreign key (sha_256) references deferred_file_body_info(sha_256);

alter table deferred_request add constraint fk_deferred_file_info_id foreign key (result_file_id) references deferred_file_info(id);
