package docs_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestRenderedNavigationContract(t *testing.T) {
	site := strings.TrimSpace(os.Getenv("FGRAPH_DOCS_BUILD_DIR"))
	if site == "" {
		t.Fatal("FGRAPH_DOCS_BUILD_DIR must point to a rendered documentation site")
	}

	root := readBuiltHTML(t, site, "index.html")
	redirect := regexp.MustCompile(`<meta http-equiv="refresh" content="0; url=[^"]*/docs/" ?/?>`)
	if !redirect.MatchString(root) {
		t.Error("built site root must redirect to the canonical Overview page at /docs/")
	}
	canonical := regexp.MustCompile(`<link rel="canonical" href="[^"]*/docs/" ?/?>`)
	if !canonical.MatchString(root) {
		t.Error("built site root must identify /docs/ as its canonical page")
	}
	for _, retired := range []string{"Read the docs", "A fact graph in a single SQLite file"} {
		if strings.Contains(root, retired) {
			t.Errorf("built site root still contains retired landing-page content %q", retired)
		}
	}

	overview := readBuiltHTML(t, site, "docs/index.html")
	if !strings.Contains(overview, `>Overview</h1>`) {
		t.Error("built /docs page must render the Overview")
	}
	readBuiltHTML(t, site, "docs/cli/index.html")
	readBuiltHTML(t, site, "docs/sdk/index.html")

	gettingStarted := readBuiltHTML(t, site, "docs/getting-started/index.html")
	mobileMarker := `<ul class="hx:flex hx:flex-col hx:gap-1 hx:md:hidden">`
	desktopMarker := `<ul class="hx:flex hx:flex-col hx:gap-1 hx:max-md:hidden">`
	mobileStart := strings.Index(gettingStarted, mobileMarker)
	desktopStart := strings.Index(gettingStarted, desktopMarker)
	if mobileStart < 0 {
		t.Fatal("built Getting Started page has no mobile sidebar list")
	}
	if desktopStart < 0 {
		t.Fatal("built Getting Started page has no desktop sidebar list")
	}
	if mobileStart >= desktopStart {
		t.Fatal("built Getting Started page places the mobile sidebar after the desktop sidebar")
	}
	desktopEnd := strings.Index(gettingStarted[desktopStart:], "</aside>")
	if desktopEnd < 0 {
		t.Fatal("built Getting Started page has an unclosed desktop sidebar")
	}

	mobile := gettingStarted[mobileStart:desktopStart]
	desktop := gettingStarted[desktopStart : desktopStart+desktopEnd]
	assertFlatDocumentationSidebar(t, "mobile", mobile)
	assertFlatDocumentationSidebar(t, "desktop", desktop)
}

func assertFlatDocumentationSidebar(t *testing.T, viewport, sidebar string) {
	t.Helper()
	labels := []string{
		"Overview",
		"Getting Started",
		"Concepts",
		"Modeling time and uncertainty",
		"Sharing and auditing memory",
		"RAG with fgraph",
		"Integrations",
		"Operations and safety boundaries",
		"CLI Reference",
		"SDK Reference",
		"fgraph v1 specification",
	}
	previous := -1
	for _, label := range labels {
		position := strings.Index(sidebar, ">"+label+"</span>")
		if position < 0 {
			t.Errorf("%s sidebar omits %q", viewport, label)
			continue
		}
		if position <= previous {
			t.Errorf("%s sidebar places %q out of order", viewport, label)
		}
		previous = position
	}
	if strings.Contains(sidebar, "SQLite format v2</span>") {
		t.Errorf("%s sidebar exposes the file-format suffix instead of the short Specification label", viewport)
	}
	if strings.Contains(sidebar, ">Documentation</span>") ||
		strings.Contains(sidebar, "hextra-sidebar-children") ||
		strings.Contains(sidebar, "hextra-sidebar-collapsible-button") {
		t.Errorf("%s sidebar must be one flat list without a Documentation wrapper", viewport)
	}
}

func readBuiltHTML(t *testing.T, site, relative string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(site, filepath.FromSlash(relative)))
	if err != nil {
		t.Fatalf("read built page %s: %v", relative, err)
	}
	return string(data)
}
