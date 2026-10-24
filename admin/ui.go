package admin

import (
	"embed"
	"io/fs"
	"net/http"
	"path"
)

// uiFiles is the web UI: one page, one style sheet, one script, all
// written for this package (no third-party code), so the UI works with no
// network access beyond the app itself.
//
//go:embed ui/index.html ui/app.css ui/app.js
var uiFiles embed.FS

// uiTypes is every file the UI serves and its media type. A path outside
// this set is a 404 even if it exists in the embed, so the set of public
// names is written down in one place.
var uiTypes = map[string]string{
	"index.html": "text/html; charset=utf-8",
	"app.css":    "text/css; charset=utf-8",
	"app.js":     "text/javascript; charset=utf-8",
}

// uiCSP lets a UI page load only its own script and style sheet and talk
// only to its own origin: no inline script, no third-party host, no
// framing (clickjacking), no form posts anywhere.
const uiCSP = "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"

// uiRedirectTarget is where GET / and GET /ui send the browser.
const uiRedirectTarget = "ui/"

// uiRedirect sends GET / and GET /ui to the UI page. The Location is
// relative, so it lands under whatever prefix the handler is mounted at
// (an absolute "/ui/" would lose a prefix that http.StripPrefix removed).
func (h *handler[P]) uiRedirect(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r, ActionUI, Resource{File: "index.html"}) {
		return
	}
	// From .../ and from .../ui alike, "ui/" resolves to .../ui/: a
	// relative reference replaces the last path segment.
	// Not http.Redirect: it rewrites a relative target against the
	// stripped request path, which would drop the mount prefix.
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Location", uiRedirectTarget)
	w.WriteHeader(http.StatusFound)
}

// uiFile serves one UI file. The files are static and hold no data, but
// they are still served only after Identify and Authorize (ActionUI): the
// gate in ServeHTTP runs before routing for every path, so an anonymous
// caller cannot even learn that the UI exists or which version it is.
func (h *handler[P]) uiFile(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("file")
	if name == "" {
		name = "index.html"
	}
	if !h.authorize(w, r, ActionUI, Resource{File: name}) {
		return
	}
	ctype, ok := uiTypes[name]
	if !ok {
		writeError(w, http.StatusNotFound, CodeNotFound, "no such UI file")
		return
	}
	body, err := fs.ReadFile(uiFiles, path.Join("ui", name))
	if err != nil {
		// Unreachable: every name in uiTypes is embedded, which
		// TestUI_FilesEmbedded checks.
		h.fail(w, ActionUI, err)
		return
	}
	hd := w.Header()
	hd.Set("Content-Type", ctype)
	hd.Set("Content-Security-Policy", uiCSP)
	hd.Set("X-Content-Type-Options", "nosniff")
	hd.Set("X-Frame-Options", "DENY")
	hd.Set("Referrer-Policy", "no-referrer")
	hd.Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}
