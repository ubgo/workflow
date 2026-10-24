# admin-ui

The admin web UI mounted in an app. `admin.NewHandler` serves it at `ui/` under its mount point, behind the same `Auth` as the JSON API. The program's `Identify` reads the app's session cookie, because a browser opening a page sends no `Authorization` header; its `Authorize` lets a viewer do every read-only action (the UI page is `admin.ActionUI`, read-only) and refuses writes. It then checks through HTTP that an anonymous caller gets 401, that `/admin/` redirects to `ui/`, that the page carries a strict Content-Security-Policy, that the runs screen's label prefix and count work, and that a viewer is refused a schedule pause an admin may do.

```sh
cd examples
go run ./admin-ui
```

```text
anonymous GET /admin/ui/: 401
viewer GET /admin/: 302 to ui/
viewer GET /admin/ui/: 200 text/html; charset=utf-8, page true
CSP: default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'
X-Frame-Options: DENY
viewer runs tenant=acme*: 2 runs, total 2
viewer pauses the schedule: 403
admin pauses the schedule: 200
```

To click through the UI, keep the server running on an address and log in through the program's stand-in login route:

```sh
ADMIN_UI_ADDR=127.0.0.1:8090 go run ./admin-ui
# then open http://127.0.0.1:8090/login?as=s-ann (admin) or ?as=s-vic (viewer)
```

The guide that copies this program is [Admin web UI](../../docs/admin/ui.md). `go test ./admin-ui` checks every line.
