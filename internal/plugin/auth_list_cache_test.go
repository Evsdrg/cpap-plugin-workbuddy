package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// The failover pool and the daily-report overlay read the host's account list
// through loadAuthList, which caches for 30s so a 429 burst does not hammer the
// host RPC. That cache has to be dropped whenever the account set changes:
// otherwise an import or a lifecycle delete keeps offering the pre-change set
// for up to 30s. These tests pin both halves — the cache, and each invalidation
// that the write/delete paths perform.

// stubAuthListSource swaps the host list provider and restores it on cleanup.
func stubAuthListSource(t *testing.T, files *[]pluginapi.HostAuthFileEntry) *int {
	t.Helper()
	previousList, previousGet := authListSource, authGetSource
	calls := 0
	authListSource = func() ([]pluginapi.HostAuthFileEntry, error) {
		calls++
		return append([]pluginapi.HostAuthFileEntry(nil), (*files)...), nil
	}
	authGetSource = func(string) (*storedAuth, error) { return nil, errors.New("no cred") }
	invalidateAuthListCache()
	t.Cleanup(func() {
		authListSource, authGetSource = previousList, previousGet
		invalidateAuthListCache()
	})
	return &calls
}

func TestLoadAuthListCachesWithinWindow(t *testing.T) {
	files := []pluginapi.HostAuthFileEntry{{AuthIndex: "idx-1", Name: "workbuddy-a.json"}}
	calls := stubAuthListSource(t, &files)

	first, err := loadAuthList()
	if err != nil || len(first) != 1 {
		t.Fatalf("first load = %v, err %v", first, err)
	}
	// A second read inside the window must be served from the cache.
	if _, err := loadAuthList(); err != nil {
		t.Fatalf("second load: %v", err)
	}
	if *calls != 1 {
		t.Fatalf("host list read %d times, want 1 (the second read should hit the cache)", *calls)
	}
}

// This is the bug the invalidation exists to prevent: without it, a changed
// account set stays invisible for the whole cache window.
func TestInvalidateAuthListCacheDropsStaleSnapshot(t *testing.T) {
	files := []pluginapi.HostAuthFileEntry{{AuthIndex: "idx-1", Name: "workbuddy-a.json"}}
	calls := stubAuthListSource(t, &files)

	if _, err := loadAuthList(); err != nil {
		t.Fatalf("warm load: %v", err)
	}

	// An account is imported behind the cache's back.
	files = append(files, pluginapi.HostAuthFileEntry{AuthIndex: "idx-2", Name: "workbuddy-b.json"})
	if got, _ := loadAuthList(); len(got) != 1 {
		t.Fatalf("cached read = %d entries, want the stale 1", len(got))
	}

	invalidateAuthListCache()
	refreshed, err := loadAuthList()
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if len(refreshed) != 2 {
		t.Fatalf("after invalidation = %d entries, want 2", len(refreshed))
	}
	if *calls != 2 {
		t.Fatalf("host list read %d times, want 2 (warm + after invalidation)", *calls)
	}
}

// A lifecycle delete removes a credential from disk; the cached list must stop
// offering it immediately, or the pool routes a request to a deleted account.
func TestDeleteAuthFileInDirInvalidatesAuthListCache(t *testing.T) {
	dir := t.TempDir()
	keep := filepath.Join(dir, "workbuddy-keep.json")
	drop := filepath.Join(dir, "workbuddy-drop.json")
	for _, path := range []string{keep, drop} {
		if err := os.WriteFile(path, []byte(`{"type":"workbuddy"}`), 0o600); err != nil {
			t.Fatalf("seed %s: %v", path, err)
		}
	}

	files := []pluginapi.HostAuthFileEntry{
		{AuthIndex: "idx-keep", Name: filepath.Base(keep)},
		{AuthIndex: "idx-drop", Name: filepath.Base(drop)},
	}
	calls := stubAuthListSource(t, &files)

	if got, err := loadAuthList(); err != nil || len(got) != 2 {
		t.Fatalf("warm load = %v, err %v", got, err)
	}

	if err := deleteAuthFileInDir(drop, dir); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := os.Stat(drop); !os.IsNotExist(err) {
		t.Fatalf("file still present after delete: %v", err)
	}

	// The next read must go back to the host rather than serve the cached pair.
	if _, err := loadAuthList(); err != nil {
		t.Fatalf("reload after delete: %v", err)
	}
	if *calls != 2 {
		t.Fatalf("host list read %d times, want 2 (the delete must drop the cache)", *calls)
	}
}

// A failed delete must NOT drop the cache: nothing changed, so re-reading buys
// nothing. The confinement checks are what fail here, before any unlink.
func TestDeleteAuthFileFailureLeavesCacheIntact(t *testing.T) {
	dir := t.TempDir()
	outside := t.TempDir() // a path that is safe by basename but outside dir
	target := filepath.Join(outside, "workbuddy-outside.json")
	if err := os.WriteFile(target, []byte(`{"type":"workbuddy"}`), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}

	files := []pluginapi.HostAuthFileEntry{{AuthIndex: "idx-1", Name: filepath.Base(target)}}
	calls := stubAuthListSource(t, &files)
	if _, err := loadAuthList(); err != nil {
		t.Fatalf("warm load: %v", err)
	}

	if err := deleteAuthFileInDir(target, dir); err == nil {
		t.Fatal("delete outside the auth dir should be refused")
	}
	if _, err := loadAuthList(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if *calls != 1 {
		t.Fatalf("host list read %d times, want 1 (a refused delete changes nothing)", *calls)
	}
}

// The invalidation is what the write path relies on, so the helper itself has to
// be safe to call before any cache was ever populated (a first-ever save must not
// panic or leave the mutex held).
func TestInvalidateAuthListCacheIsSafeBeforeFirstLoad(t *testing.T) {
	invalidateAuthListCache()
	done := make(chan struct{})
	go func() {
		invalidateAuthListCache()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("invalidateAuthListCache deadlocked on an unpopulated cache")
	}
}
