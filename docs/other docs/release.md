# IGEM release process

The repository ships **three** independently versioned artefacts:

| artefact         | distribution               | tag pattern   | example         |
|------------------|----------------------------|---------------|-----------------|
| Client package   | PyPI · `igem`              | `client-v*`   | `client-v2.1.0` |
| Server package   | PyPI · `igem-server`       | `server-v*`   | `server-v0.1.0` |
| Container image  | GHCR · `ghcr.io/<org>/igem`| `container-v*`| `container-v1.0.0` |

Each tag triggers exactly one workflow under `.github/workflows/` —
they are independent and never block each other.

Container versions are deliberately decoupled from package versions.
The container is a deployment artefact; its tag changes when the
image itself changes (base image, system deps, entrypoint, …) — not
necessarily when client/server packages change. The bundled package
versions are recorded as image labels (`igem.client_version`,
`igem.server_version`).

## Release a new client (`igem`) version

1. Update `client/pyproject.toml`:
   ```toml
   version = "2.1.1"
   ```
2. Commit the bump on `main`:
   ```bash
   git add client/pyproject.toml
   git commit -m "chore(client): release 2.1.1"
   git push origin main
   ```
3. Tag and push:
   ```bash
   git tag client-v2.1.1
   git push origin client-v2.1.1
   ```
4. CI will:
   - Verify `client/pyproject.toml` version matches the tag
   - Build wheel + sdist
   - Publish to PyPI as `igem`
   - Show a summary in the GitHub Actions run

If the version check fails, the workflow stops before publishing — fix
the pyproject and re-tag (no risk of accidental upload).

## Release a new server (`igem-server`) version

Same flow as client, using the `server-v*` tag and `backend/`:

```bash
# bump version in backend/pyproject.toml
git tag server-v0.2.0
git push origin server-v0.2.0
```

## Release a new container

The container build pulls whatever is currently checked in for client
and server. So a typical sequence is:

```bash
# Already have client 2.1.0 + server 0.1.0 published.
# Release a container that bundles them:
git tag container-v1.0.0
git push origin container-v1.0.0
```

The workflow records the embedded versions as image labels:

```bash
$ docker inspect ghcr.io/<org>/igem:1.0.0 \
    | jq '.[0].Config.Labels'
{
  "igem.client_version": "2.1.0",
  "igem.server_version": "0.1.0",
  "org.opencontainers.image.version": "1.0.0",
  ...
}
```

## Required GitHub secrets

| secret             | used by                      | description                        |
|--------------------|------------------------------|------------------------------------|
| `PYPI_TOKEN_CLIENT`| `publish-client.yml`         | PyPI scoped token for `igem`       |
| `PYPI_TOKEN_SERVER`| `publish-server.yml`         | PyPI scoped token for `igem-server`|

The container workflow uses the auto-provided `GITHUB_TOKEN` and
publishes to GHCR (no extra secret needed).

To set up:
1. Create a PyPI account, register projects `igem` and `igem-server`
2. Generate two scoped API tokens (one per project) at
   <https://pypi.org/manage/account/token/>
3. Add them as repository secrets under
   *Settings → Secrets → Actions*

## Pre-release checklist

Before tagging:

- [ ] `pyproject.toml` `version` field bumped, committed, pushed
- [ ] Local build succeeds: `cd <package> && poetry build`
- [ ] Local install succeeds: `pip install dist/*.whl`
- [ ] CHANGELOG entry written (if you keep one)
- [ ] Tests pass on `main`
- [ ] Tag matches pyproject version exactly (CI will refuse otherwise)

## Version conventions (semver)

| change                                  | bump          |
|-----------------------------------------|---------------|
| Bug fix, no API change                  | patch (`x.y.Z`) |
| New feature, backward-compatible        | minor (`x.Y.z`) |
| Breaking API change                     | major (`X.y.z`) |
| Container: change in base image / deps  | minor         |
| Container: only embedded packages change| patch         |
