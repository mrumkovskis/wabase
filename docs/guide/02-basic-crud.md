# Part 2: Data Model & Basic CRUD

This part defines metadata in the runtime folders Wabase loads by default: `tables/`, `views/`, `routes/`.

## 1. Define Table Metadata

Create `src/main/resources/tables/tms.yaml`:

```yaml
table: tms_user
columns:
- id         ! 12
- username   ! 80
- full_name  ! 120
- email        160
- is_active  ! boolean
pk:
- id
idx:
- username


table: project
columns:
- id          ! 12
- name        ! 160
- description   2000
- owner_id    ! tms_user.id
- status      ! 20
pk:
- id


table: task
columns:
- id          ! 12
- project_id  ! project.id
- assignee_id ! tms_user.id
- summary     ! 240
- details       4000
- due_date      date
- priority    ! 20
- status      ! 20
pk:
- id
idx:
- project_id
- assignee_id
```

## 2. Create Physical DB Schema

Create and apply `db/schema.sql`:

```sql
create table tms_user (
  id bigserial primary key,
  username varchar(80) not null unique,
  full_name varchar(120) not null,
  email varchar(160),
  is_active boolean not null default true
);

create table project (
  id bigserial primary key,
  name varchar(160) not null,
  description text,
  owner_id bigint not null references tms_user(id),
  status varchar(20) not null default 'PLANNING'
);

create table task (
  id bigserial primary key,
  project_id bigint not null references project(id),
  assignee_id bigint not null references tms_user(id),
  summary varchar(240) not null,
  details text,
  due_date date,
  priority varchar(20) not null default 'MEDIUM',
  status varchar(20) not null default 'OPEN'
);
```

Apply it:

```bash
psql -U tms_user -d tms_db -f db/schema.sql
```

## 3. Define Views

Create `src/main/resources/views/tms.yaml`:

```yaml
name: user
table: tms_user
api: count, create, get, list, save, delete
key: id
fields:
- id
- username
- full_name
- email
- is_active
filter:
- username ~% :username?
order:
- full_name


name: project
table: project
api: count, create, get, list, save, delete
key: id
fields:
- id
- name
- description
- owner_id
- status
order:
- id desc


name: task
table: task
api: count, create, get, list, save, delete
key: id
fields:
- id
- project_id
- assignee_id
- summary
- details
- due_date
- priority
- status
order:
- id desc
```

## 4. Define Routes

Create `src/main/resources/routes/data.yaml`:

```yaml
on: /api/((?:create:|count:)?\w+)(/.+)?
do: doAction $1
```

## 5. Run and Verify

```bash
sbt run
```

Quick checks:

```bash
curl -X POST http://localhost:8080/api/user \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","full_name":"Alice Smith","email":"alice@example.com","is_active":true}'

curl http://localhost:8080/api/user
curl http://localhost:8080/api/user/1
```

**Next Step:** [Relationships and Validation](03-relationships-and-validation.md)
