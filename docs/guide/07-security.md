# Part 7: Security & Roles

Wabase supports role-based API control with route-level authentication handlers.

## 1. Role-Scoped API Methods

In view metadata, prefix methods with role names:

```yaml
name: user
table: tms_user
api: admin list delete, user get save
key: id
fields:
- id
- username
- full_name
```

Meaning:
1. `admin` can call `list` and `delete`.
2. `user` can call `get` and `save`.

## 2. Login/Logout Views

Create `src/main/resources/views/security.yaml`:

```yaml
name: login_using_json_or_urlencoded
api: insert
fields:
- name
- password
insert:
- if exists(tms_user[username = :current_user.credentials.name] {id}) :
  - user_id = tms_user[username = :current_user.credentials.name] {id}
  - (status ok
      user_attrs({'id', :user_id})
      user_attrs({'name', :current_user.credentials.name})
    )
- else:
  - status 401


name: logout
api: insert
insert:
- status ok
```

## 3. Security Routes

Create `src/main/resources/routes/security.yaml`:

```yaml
on: /(login_using_json_or_urlencoded)
do: extractFormDataCredentials setDomainAndPathSessionCookie(null, '/') doAction $1

on: POST /logout
do: removeSessionCookie doAction('logout')

on: /role-check/(.+)
do: authenticateOpt checkRole($1) ok
```

If you use `checkRole`, ensure your login flow writes `roles` into session via `user_attrs({'roles', ...})`.

## 4. Row-Level Filtering

Use `:current_user` in view filters:

```yaml
name: my_tasks
table: task
api: get, list
key: id
fields:
- id
- summary
- assignee_id
filter:
- assignee_id = :current_user.id
```

`authenticate`/`authenticateOpt` must run before `doAction` for `:current_user` to be populated.

## 5. CSRF for Browser Clients

If your client is browser-based, add CSRF checks on state-changing routes:

```yaml
on: POST /ui/(.+)
do: checkSameOrigin checkCsrfToken authenticate doAction $1
```

**Next Step:** [Deployment](08-deployment.md)
