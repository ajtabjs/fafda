package bolt

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"go.etcd.io/bbolt"

	"fafda/internal"
)

func setupTestDB(t *testing.T) (internal.MetaFileSystem, func()) {
	tmpFile := filepath.Join(t.TempDir(), "test.db")
	db, err := bbolt.Open(tmpFile, 0600, nil)
	if err != nil {
		t.Fatalf("failed to open bolt")
	}

	provider, err := NewMetaFs(db)
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}

	cleanup := func() {
		_ = provider.Close()
		_ = os.Remove(tmpFile)
	}

	return provider, cleanup
}

func TestCreate(t *testing.T) {
	provider, cleanup := setupTestDB(t)
	defer cleanup()

	tests := []struct {
		name    string
		path    string
		isDir   bool
		wantErr error
	}{
		{
			name:    "create directory",
			path:    "/test",
			isDir:   true,
			wantErr: nil,
		},
		{
			name:    "create file in directory",
			path:    "/test/file.txt",
			isDir:   false,
			wantErr: nil,
		},
		{
			name:    "create duplicate directory",
			path:    "/test",
			isDir:   true,
			wantErr: internal.ErrAlreadyExist,
		},
		{
			name:    "create duplicate file",
			path:    "/test/file.txt",
			isDir:   false,
			wantErr: internal.ErrAlreadyExist,
		},
		{
			name:    "create in missing directory",
			path:    "/missing/file.txt",
			isDir:   false,
			wantErr: internal.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file, err := provider.Create(tt.path, tt.isDir)
			if tt.wantErr != nil {
				if err == nil {
					t.Error("expected error but got none")
					return
				}
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("got error %q, want %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if file.IsDir() != tt.isDir {
				t.Errorf("got isDir %v, want %v", file.IsDir(), tt.isDir)
			}
			if file.Name() != filepath.Base(tt.path) {
				t.Errorf("got name %q, want %q", file.Name(), filepath.Base(tt.path))
			}
		})
	}
}

func TestStat(t *testing.T) {
	provider, cleanup := setupTestDB(t)
	defer cleanup()

	// Setup test directory and file
	_, err := provider.Create("/test", true)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	_, err = provider.Create("/test/file.txt", false)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	tests := []struct {
		name    string
		path    string
		wantDir bool
		wantErr bool
	}{
		{
			name:    "stat root",
			path:    "/",
			wantDir: true,
			wantErr: false,
		},
		{
			name:    "stat directory",
			path:    "/test",
			wantDir: true,
			wantErr: false,
		},
		{
			name:    "stat file",
			path:    "/test/file.txt",
			wantDir: false,
			wantErr: false,
		},
		{
			name:    "stat non-existent",
			path:    "/missing",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, err := provider.Stat(tt.path)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if info.IsDir() != tt.wantDir {
				t.Errorf("got isDir %v, want %v", info.IsDir(), tt.wantDir)
			}
		})
	}
}

func TestLs(t *testing.T) {
	provider, cleanup := setupTestDB(t)
	defer cleanup()

	// Setup test structure
	_, err := provider.Create("/test", true)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	files := []string{"a.txt", "b.txt", "c.txt"}
	for _, f := range files {
		_, err := provider.Create("/test/"+f, false)
		if err != nil {
			t.Fatalf("setup failed: %v", err)
		}
	}

	tests := []struct {
		name      string
		path      string
		limit     int
		offset    int
		wantCount int
		wantErr   bool
	}{
		{
			name:      "list all files",
			path:      "/test",
			limit:     0,
			offset:    0,
			wantCount: 3,
		},
		{
			name:      "list with limit",
			path:      "/test",
			limit:     2,
			offset:    0,
			wantCount: 2,
		},
		{
			name:      "list with offset",
			path:      "/test",
			limit:     0,
			offset:    1,
			wantCount: 2,
		},
		{
			name:    "list non-existent directory",
			path:    "/missing",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			files, err := provider.Ls(tt.path, tt.limit, tt.offset)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if len(files) != tt.wantCount {
				t.Errorf("got %d files, want %d", len(files), tt.wantCount)
			}
		})
	}
}

func TestRemove(t *testing.T) {
	provider, cleanup := setupTestDB(t)
	defer cleanup()

	tests := []struct {
		name    string
		setup   func() error
		cleanup func() error
		path    string
		wantErr error
	}{
		{
			name: "remove file",
			setup: func() error {
				if _, err := provider.Create("/test", true); err != nil {
					return err
				}
				if _, err := provider.Create("/test/file.txt", false); err != nil {
					return err
				}
				return nil
			},
			cleanup: func() error {
				_ = provider.Remove("/test/file.txt") // Ignore errors as file might be already removed
				_ = provider.Remove("/test")
				return nil
			},
			path: "/test/file.txt",
		},
		{
			name: "remove empty directory",
			setup: func() error {
				_, err := provider.Create("/empty", true)
				return err
			},
			cleanup: func() error {
				_ = provider.Remove("/empty")
				return nil
			},
			path: "/empty",
		},
		{
			name: "remove non-empty directory",
			setup: func() error {
				if _, err := provider.Create("/test2", true); err != nil {
					return err
				}
				if _, err := provider.Create("/test2/file.txt", false); err != nil {
					return err
				}
				return nil
			},
			cleanup: func() error {
				_ = provider.Remove("/test2/file.txt")
				_ = provider.Remove("/test2")
				return nil
			},
			path:    "/test2",
			wantErr: internal.ErrNotEmpty,
		},
		{
			name:    "remove non-existent",
			setup:   func() error { return nil },
			cleanup: func() error { return nil },
			path:    "/missing",
			wantErr: internal.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.setup(); err != nil {
				t.Fatalf("setup failed: %v", err)
			}

			if info, err := provider.Stat(tt.path); err == nil && info.IsDir() {
				files, err := provider.Ls(tt.path, 0, 0)
				if err != nil {
					t.Logf("Failed to list directory: %v", err)
				} else {
					t.Logf("Directory %s contains %d files", tt.path, len(files))
					for _, f := range files {
						t.Logf("- %s", f.Name())
					}
				}
			}

			err := provider.Remove(tt.path)

			defer func() {
				if err := tt.cleanup(); err != nil {
					t.Logf("cleanup failed: %v", err)
				}
			}()

			if tt.wantErr != nil {
				if err == nil {
					t.Error("expected error but got none")
					return
				}
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("got error %q, want %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if _, err := provider.Stat(tt.path); err == nil {
				t.Error("path still exists after removal")
			}
		})
	}
}

func TestRename(t *testing.T) {
	provider, cleanup := setupTestDB(t)
	defer cleanup()

	tests := []struct {
		name    string
		setup   func() error
		cleanup func() error
		oldpath string
		newpath string
		wantErr error
	}{
		{
			name: "rename file",
			setup: func() error {
				if _, err := provider.Create("/test", true); err != nil {
					return err
				}
				if _, err := provider.Create("/test/file.txt", false); err != nil {
					return err
				}
				return nil
			},
			cleanup: func() error {
				_ = provider.Remove("/test/file.txt")
				_ = provider.Remove("/test/newfile.txt")
				_ = provider.Remove("/test")
				return nil
			},
			oldpath: "/test/file.txt",
			newpath: "/test/newfile.txt",
		},
		{
			name: "rename directory",
			setup: func() error {
				if _, err := provider.Create("/dir1", true); err != nil {
					return err
				}
				if _, err := provider.Create("/dir1/file.txt", false); err != nil {
					return err
				}
				return nil
			},
			cleanup: func() error {
				_ = provider.Remove("/dir1/file.txt")
				_ = provider.Remove("/dir1")
				_ = provider.Remove("/dir2/file.txt")
				_ = provider.Remove("/dir2")
				return nil
			},
			oldpath: "/dir1",
			newpath: "/dir2",
		},
		{
			name:    "rename non-existent",
			setup:   func() error { return nil },
			cleanup: func() error { return nil },
			oldpath: "/missing",
			newpath: "/new",
			wantErr: internal.ErrNotFound,
		},
		{
			name: "rename to existing path",
			setup: func() error {
				if _, err := provider.Create("/test2", true); err != nil {
					return err
				}
				if _, err := provider.Create("/test2/file.txt", false); err != nil {
					return err
				}
				return nil
			},
			cleanup: func() error {
				_ = provider.Remove("/test2/file.txt")
				_ = provider.Remove("/test2")
				return nil
			},
			oldpath: "/test2/file.txt",
			newpath: "/test2",
			wantErr: internal.ErrAlreadyExist,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.setup(); err != nil {
				t.Fatalf("setup failed: %v", err)
			}
			defer tt.cleanup()

			err := provider.Rename(tt.oldpath, tt.newpath)
			if tt.wantErr != nil {
				if err == nil {
					t.Error("expected error but got none")
					return
				}
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("got error %q, want %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if _, err := provider.Stat(tt.oldpath); err == nil {
				t.Error("source still exists after rename")
			}

			if _, err := provider.Stat(tt.newpath); err != nil {
				t.Error("destination doesn't exist after rename")
			}
		})
	}
}

func TestComplexRenameOperations(t *testing.T) {
	provider, cleanup := setupTestDB(t)
	defer cleanup()

	// Setup structure:
	// /project
	//   /src
	//     /main.go
	//     /lib
	//       /util.go
	//   /test
	//     /main_test.go
	//   /docs
	//     /readme.md

	dirs := []string{
		"/project",
		"/project/src",
		"/project/src/lib",
		"/project/test",
		"/project/docs",
	}

	files := []string{
		"/project/src/main.go",
		"/project/src/lib/util.go",
		"/project/test/main_test.go",
		"/project/docs/readme.md",
	}

	for _, dir := range dirs {
		_, err := provider.Create(dir, true)
		if err != nil {
			t.Fatalf("failed to create dir %s: %v", dir, err)
		}
	}

	for _, file := range files {
		_, err := provider.Create(file, false)
		if err != nil {
			t.Fatalf("failed to create file %s: %v", file, err)
		}
	}

	// 1. Move src/lib to src/utils
	err := provider.Rename("/project/src/lib", "/project/src/utils")
	if err != nil {
		t.Fatalf("failed to rename lib to utils: %v", err)
	}

	// 2. Move entire test directory into src
	err = provider.Rename("/project/test", "/project/src/test")
	if err != nil {
		t.Fatalf("failed to move test into src: %v", err)
	}

	// 3. Move src directory to root
	err = provider.Rename("/project/src", "/src")
	if err != nil {
		t.Fatalf("failed to move src to root: %v", err)
	}

	// Verify final structure
	expectedPaths := []string{
		"/src/main.go",
		"/src/utils/util.go",
		"/src/test/main_test.go",
		"/project/docs/readme.md",
	}

	for _, path := range expectedPaths {
		if _, err := provider.Stat(path); err != nil {
			t.Errorf("expected path %s not found: %v", path, err)
		}
	}
}

func TestMkdirAll(t *testing.T) {
	provider, cleanup := setupTestDB(t)
	defer cleanup()

	tests := []struct {
		name    string
		path    string
		setup   func() error
		wantErr bool
	}{
		{
			name: "create single directory",
			path: "/test",
		},
		{
			name: "create nested directories",
			path: "/a/b/c/d",
		},
		{
			name: "create already existing directory",
			path: "/existing",
			setup: func() error {
				_, err := provider.Create("/existing", true)
				return err
			},
		},
		{
			name: "create with existing parent",
			path: "/parent/child",
			setup: func() error {
				_, err := provider.Create("/parent", true)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setup != nil {
				if err := tt.setup(); err != nil {
					t.Fatalf("setup failed: %v", err)
				}
			}

			err := provider.MkdirAll(tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("MkdirAll() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// Verify directory exists
			info, err := provider.Stat(tt.path)
			if err != nil {
				t.Errorf("failed to stat created directory: %v", err)
				return
			}
			if !info.IsDir() {
				t.Error("created path is not a directory")
			}

			parent := filepath.Dir(tt.path)
			for parent != "/" {
				info, err := provider.Stat(parent)
				if err != nil {
					t.Errorf("parent directory %s not found: %v", parent, err)
				} else if !info.IsDir() {
					t.Errorf("parent path %s is not a directory", parent)
				}
				parent = filepath.Dir(parent)
			}
		})
	}
}

func TestRemoveAll(t *testing.T) {
	provider, cleanup := setupTestDB(t)
	defer cleanup()

	tests := []struct {
		name    string
		setup   func() error
		path    string
		wantErr bool
	}{
		{
			name: "remove single directory",
			setup: func() error {
				_, err := provider.Create("/test", true)
				return err
			},
			path: "/test",
		},
		{
			name: "remove nested structure",
			setup: func() error {
				if err := provider.MkdirAll("/a/b/c"); err != nil {
					return err
				}
				if _, err := provider.Create("/a/b/c/file1.txt", false); err != nil {
					return err
				}
				if _, err := provider.Create("/a/b/file2.txt", false); err != nil {
					return err
				}
				return nil
			},
			path: "/a",
		},
		{
			name:    "remove root",
			path:    "/",
			wantErr: true,
		},
		{
			name: "remove non-existent path",
			path: "/missing",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setup != nil {
				if err := tt.setup(); err != nil {
					t.Fatalf("setup failed: %v", err)
				}
			}

			err := provider.RemoveAll(tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("RemoveAll() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if _, err := provider.Stat(tt.path); err == nil {
					t.Error("path still exists after removal")
				}

				files, err := provider.Ls(filepath.Dir(tt.path), 0, 0)
				if err == nil {
					for _, f := range files {
						if strings.HasPrefix(f.Name(), filepath.Base(tt.path)) {
							t.Errorf("found remaining file: %s", f.Name())
						}
					}
				}
			}
		})
	}
}

func TestDeepDirectoryOperations(t *testing.T) {
	provider, cleanup := setupTestDB(t)
	defer cleanup()

	// /a/b/c/d/e/file.txt
	path := ""
	dirs := []string{"a", "b", "c", "d", "e"}
	for _, dir := range dirs {
		path = filepath.Join(path, dir)
		fullPath := "/" + path
		_, err := provider.Create(fullPath, true)
		if err != nil {
			t.Fatalf("failed to create %s: %v", fullPath, err)
		}
	}

	deepFile := "/a/b/c/d/e/file.txt"
	_, err := provider.Create(deepFile, false)
	if err != nil {
		t.Fatalf("failed to create deep file: %v", err)
	}

	// Move middle directory
	// /a/b/c/d/e/file.txt -> /a/b/x/d/e/file.txt
	err = provider.Rename("/a/b/c", "/a/b/x")
	if err != nil {
		t.Fatalf("failed to move middle directory: %v", err)
	}

	_, err = provider.Stat("/a/b/x/d/e/file.txt")
	if err != nil {
		t.Error("file not found in new location")
	}

	_, err = provider.Stat(deepFile)
	if err == nil {
		t.Error("old path still exists")
	}
}

func TestConcurrentOperations(t *testing.T) {
	provider, cleanup := setupTestDB(t)
	defer cleanup()

	// Create base directory first
	_, err := provider.Create("/shared", true)
	if err != nil {
		t.Fatal(err)
	}

	// Create all directories first
	for i := 0; i < 10; i++ {
		dirPath := fmt.Sprintf("/shared/dir%d", i)
		_, err := provider.Create(dirPath, true)
		if err != nil {
			t.Fatalf("failed to create directory %s: %v", dirPath, err)
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, 100)

	// Now create files concurrently
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			dirPath := fmt.Sprintf("/shared/dir%d", i)
			// Create files in directory
			for j := 0; j < 5; j++ {
				filePath := fmt.Sprintf("%s/file%d.txt", dirPath, j)
				_, err := provider.Create(filePath, false)
				if err != nil {
					errs <- fmt.Errorf("failed to create %s: %v", filePath, err)
					return
				}
			}
		}(i)
	}

	wg.Wait()

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			oldPath := fmt.Sprintf("/shared/dir%d", i)
			newPath := fmt.Sprintf("/shared/dir%d_renamed", i)
			err := provider.Rename(oldPath, newPath)
			if err != nil {
				errs <- fmt.Errorf("failed to rename %s to %s: %v", oldPath, newPath, err)
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	var testErrors []string
	for err := range errs {
		testErrors = append(testErrors, err.Error())
	}
	if len(testErrors) > 0 {
		t.Errorf("encountered errors:\n%s", strings.Join(testErrors, "\n"))
	}

	files, err := provider.Ls("/shared", 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	if len(files) != 10 {
		t.Errorf("expected 10 directories, got %d", len(files))
	}

	for i := 0; i < 5; i++ {
		newPath := fmt.Sprintf("/shared/dir%d_renamed", i)
		files, err := provider.Ls(newPath, 0, 0)
		if err != nil {
			t.Errorf("failed to list %s: %v", newPath, err)
			continue
		}
		if len(files) != 5 {
			t.Errorf("expected 5 files in %s, got %d", newPath, len(files))
		}
	}
}

func TestEdgeCases(t *testing.T) {
	provider, cleanup := setupTestDB(t)
	defer cleanup()

	cleanAll := func(t *testing.T) {
		paths := []string{
			"/dir",
			"/dir1",
			"/dir2",
			"/file.txt",
			"/dir/file.txt",
			"/dir/subfile",
			"/missing",
		}
		for _, path := range paths {
			_ = provider.Remove(path)
		}
	}

	tests := []struct {
		name    string
		setup   func(t *testing.T)
		test    func() error
		wantErr error
	}{
		{
			name: "move directory into itself",
			setup: func(t *testing.T) {
				cleanAll(t)
				_, err := provider.Create("/dir", true)
				if err != nil {
					t.Fatalf("create dir failed: %v", err)
				}
				_, err = provider.Create("/dir/file.txt", false)
				if err != nil {
					t.Fatalf("create file failed: %v", err)
				}
			},
			test: func() error {
				return provider.Rename("/dir", "/dir/subdir")
			},
			wantErr: internal.ErrInvalidOperation,
		},
		{
			name: "rename root directory",
			setup: func(t *testing.T) {
				cleanAll(t)
			},
			test: func() error {
				return provider.Rename("/", "/newroot")
			},
			wantErr: internal.ErrInvalidRootOperation,
		},
		{
			name: "rename to existing path",
			setup: func(t *testing.T) {
				cleanAll(t)
				_, err := provider.Create("/dir1", true)
				if err != nil {
					t.Fatalf("create dir1 failed: %v", err)
				}
				_, err = provider.Create("/dir2", true)
				if err != nil {
					t.Fatalf("create dir2 failed: %v", err)
				}
			},
			test: func() error {
				return provider.Rename("/dir1", "/dir2")
			},
			wantErr: internal.ErrAlreadyExist,
		},
		{
			name: "rename with missing parent",
			setup: func(t *testing.T) {
				cleanAll(t)
				_, err := provider.Create("/dir", true)
				if err != nil {
					t.Fatalf("create dir failed: %v", err)
				}
			},
			test: func() error {
				return provider.Rename("/dir", "/missing/dir")
			},
			wantErr: internal.ErrNotFound,
		},
		{
			name: "rename to file path",
			setup: func(t *testing.T) {
				cleanAll(t)
				_, err := provider.Create("/dir", true)
				if err != nil {
					t.Fatalf("create dir failed: %v", err)
				}
				_, err = provider.Create("/dir/subfile", false)
				if err != nil {
					t.Fatalf("create subfile failed: %v", err)
				}
				_, err = provider.Create("/file.txt", false)
				if err != nil {
					t.Fatalf("create file.txt failed: %v", err)
				}
			},
			test: func() error {
				return provider.Rename("/dir", "/file.txt")
			},
			wantErr: internal.ErrAlreadyExist,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup(t)

			err := tt.test()
			if err == nil {
				t.Error("expected error but got none")
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("got error %q, want %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestVirtualFileSystemOperations(t *testing.T) {
	fs, cleanup := setupTestDB(t)
	defer cleanup()

	t.Run("1. Initial ls", func(t *testing.T) {
		files, err := fs.Ls("/", 10, 0)
		if err != nil {
			t.Fatalf("Initial ls failed: %v", err)
		}
		if len(files) != 0 {
			t.Error("Root directory should be empty")
		}
	})

	t.Run("2. mkdir abc", func(t *testing.T) {
		if err := fs.Mkdir("/abc"); err != nil {
			t.Fatalf("Failed to create abc directory: %v", err)
		}

		stat, err := fs.Stat("/abc")
		if err != nil {
			t.Fatalf("Failed to stat abc directory: %v", err)
		}
		if !stat.IsDir() {
			t.Error("abc should be a directory")
		}
	})

	t.Run("3. ls abc", func(t *testing.T) {
		files, err := fs.Ls("/abc", 10, 0)
		if err != nil {
			t.Fatalf("Failed to list abc directory: %v", err)
		}
		if len(files) != 0 {
			t.Error("abc directory should be empty")
		}
	})

	t.Run("4. mkdir abc/hello", func(t *testing.T) {
		if err := fs.Mkdir("/abc/hello"); err != nil {
			t.Fatalf("Failed to create abc/hello directory: %v", err)
		}

		stat, err := fs.Stat("/abc/hello")
		if err != nil {
			t.Fatalf("Failed to stat abc/hello directory: %v", err)
		}
		if !stat.IsDir() {
			t.Error("abc/hello should be a directory")
		}
	})

	t.Run("5. touch abc/hello/abc.txt", func(t *testing.T) {
		_, err := fs.Create("/abc/hello/abc.txt", false)
		if err != nil {
			t.Fatalf("Failed to create abc/hello/abc.txt: %v", err)
		}

		stat, err := fs.Stat("/abc/hello/abc.txt")
		if err != nil {
			t.Fatalf("Failed to stat abc/hello/abc.txt: %v", err)
		}
		if stat.IsDir() {
			t.Error("abc.txt should be a file")
		}
	})

	t.Run("6. rename abc/hello abc/xyz", func(t *testing.T) {
		if err := fs.Rename("/abc/hello", "/abc/xyz"); err != nil {
			t.Fatalf("Failed to rename hello to xyz: %v", err)
		}

		if _, err := fs.Stat("/abc/hello"); err == nil {
			t.Error("Old path should not exist")
		}

		stat, err := fs.Stat("/abc/xyz")
		if err != nil {
			t.Fatalf("Failed to stat new path: %v", err)
		}
		if !stat.IsDir() {
			t.Error("xyz should be a directory")
		}

		if _, err := fs.Stat("/abc/xyz/abc.txt"); err != nil {
			t.Fatalf("File should exist in new location: %v", err)
		}
	})

	t.Run("7. rename abc/xyz/abc.txt abc/abc.txt", func(t *testing.T) {
		if err := fs.Rename("/abc/xyz/abc.txt", "/abc/abc.txt"); err != nil {
			t.Fatalf("Failed to move abc.txt: %v", err)
		}

		stat, err := fs.Stat("/abc/abc.txt")
		if err != nil {
			t.Fatalf("Failed to stat moved file: %v", err)
		}
		if stat.IsDir() {
			t.Error("abc.txt should be a file")
		}

		if _, err := fs.Stat("/abc/xyz/abc.txt"); err == nil {
			t.Error("File should not exist in old location")
		}
	})

	t.Run("8. rename abc/xyz xyz", func(t *testing.T) {
		if err := fs.Rename("/abc/xyz", "/xyz"); err != nil {
			t.Fatalf("Failed to move xyz directory: %v", err)
		}

		stat, err := fs.Stat("/xyz")
		if err != nil {
			t.Fatalf("Failed to stat moved directory: %v", err)
		}
		if !stat.IsDir() {
			t.Error("xyz should be a directory")
		}

		if _, err := fs.Stat("/abc/xyz"); err == nil {
			t.Error("Directory should not exist in old location")
		}
	})

	t.Run("9. sync abc/abc.txt 100", func(t *testing.T) {
		if err := fs.Sync("/abc/abc.txt", 100); err != nil {
			t.Fatalf("Failed to sync file size: %v", err)
		}

		stat, err := fs.Stat("/abc/abc.txt")
		if err != nil {
			t.Fatalf("Failed to stat file: %v", err)
		}
		if stat.Size() != 100 {
			t.Errorf("File size should be 100, got %d", stat.Size())
		}
	})

	t.Run("10. rename abc/abc.txt abc.txt", func(t *testing.T) {
		if err := fs.Rename("/abc/abc.txt", "/abc.txt"); err != nil {
			t.Fatalf("Failed to move abc.txt to root: %v", err)
		}

		stat, err := fs.Stat("/abc.txt")
		if err != nil {
			t.Fatalf("Failed to stat moved file: %v", err)
		}
		if stat.IsDir() {
			t.Error("abc.txt should be a file")
		}
		if stat.Size() != 100 {
			t.Errorf("File size should remain 100, got %d", stat.Size())
		}

		if _, err := fs.Stat("/abc/abc.txt"); err == nil {
			t.Error("File should not exist in old location")
		}
	})
}
