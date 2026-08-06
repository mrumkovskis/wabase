# Testing and Verification Playbook

This chapter explains how the repository verifies framework behavior across unit tests, service tests, and integration route scenarios.

## 1. Test Projects

`build.sbt` defines three main test scopes:
1. `wabase` (unit and component tests in `src/test`)
2. `it` (integration project in `src/it`)
3. `it_legacy` (legacy compatibility integration project in `src/it_legacy`)

CI executes:
- `+test`
- `+it/test`
- `+it_legacy/test`
- `+versionPolicyCheck`

## 2. Fast Local Commands

```bash
# core tests
sbt test

# integration tests
sbt it/test

# legacy integration tests
sbt it_legacy/test

# full pipeline (close to CI)
sbt clean update +compile +test:compile +test +it/test:compile +it/test +it_legacy/test:compile +it_legacy/test +versionPolicyCheck
```

## 3. Core Unit Spec Areas (`src/test/scala`)

Representative focus areas:
- Authentication/session/JWT/key loading
- Route parsing and route execution
- Request decoding / unmarshalling / marshalling
- Deferred processing and serializer streams
- DB access, CRUD and metadata filters
- Swagger merge generation
- File upload, buffer flow, cleanup
- Script validation and i18n behavior

## 4. Integration Scenario Model (`src/it/resources/http_tests`)

Integration tests are organized by capability folders:
- `auth`, `csrf-defence`, `deferred`, `job`, `i18n`
- `person-crud`, `user-crud-by-id`, `guidelines`
- `form`, `template`, `get-from-resource`, `api`

Each YAML file typically encodes:
1. Request method/path/body/headers
2. Expected HTTP status + response semantics
3. Stateful sequencing when needed (setup, action, teardown)

## 5. Integration Runtime Resources

`src/it/resources` provides the in-test app definition:
- `tables/*.yaml` (schema model)
- `views/*.yaml` (view/action definitions)
- `routes/*.yaml` (route chains)
- `reference.conf`, `application.sample.conf`

This setup is useful as a reference implementation for framework consumers.

## 6. Adding a New Feature Test

Suggested workflow:
1. Add/adjust table/view/route definition in `src/it/resources`.
2. Add one or more scenario files in the most relevant `http_tests/<feature>/` folder.
3. Add unit specs in `src/test/scala` for parser/utility edge cases.
4. Run `sbt it/test` and `sbt test`.
5. If behavior is legacy-sensitive, mirror scenario in `src/it_legacy` where applicable.

## 7. Failure Debugging

1. Use single-suite execution when narrowing failures:

```bash
sbt 'testOnly org.wabase.AuthenticationSpecs'
sbt 'it/testOnly *BusinessScenariosSpecs'
```

2. Inspect generated test reports (`*-report` folders).
3. Correlate failing route behavior with corresponding YAML route/view files.
4. Validate parser and renderer settings in test `reference.conf` / `application.conf`.

## 8. Coverage Map Pointers

For file-level feature mapping, use:
- `docs/reference/11-feature-coverage-index.md`
- `docs/features/00-feature-catalog.md`
