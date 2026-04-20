// Package flyfile provides an interface to FlyFile storage (write-only).
package flyfile

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/backend/flyfile/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
)

func init() {
	fs.Register(&fs.RegInfo{
		Name:        "flyfile",
		Description: "FlyFile",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:      "api_url",
			Help:      "FlyFile API base URL (e.g. https://api.myflyfile.com)",
			Required:  true,
			Sensitive: false,
		}, {
			Name:      "api_key",
			Help:      "FlyFile API key (x-api-key)",
			Required:  true,
			Sensitive: true,
		}, {
			Name:      "upload_url",
			Help:      "FlyFile uploader service URL (defaults to api_url if empty)",
			Required:  false,
			Sensitive: false,
		}},
	})
}

// Options holds the configuration for the FlyFile backend
type Options struct {
	APIURL    string `config:"api_url"`
	APIKey    string `config:"api_key"`
	UploadURL string `config:"upload_url"`
}

// Fs represents a remote FlyFile
type Fs struct {
	name       string
	root       string
	opt        Options
	features   *fs.Features
	client     *api.Client
	httpClient *http.Client
	// folder path → id cache
	folderCache   map[string]string
	folderCacheMu sync.Mutex
	folderCacheAt time.Time
}

// Object represents a FlyFile file
type Object struct {
	fs      *Fs
	remote  string
	size    int64
	modTime time.Time
	id      string
}

// NewFs creates a new FlyFile Fs
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	if err := configstruct.Set(m, opt); err != nil {
		return nil, err
	}
	if opt.UploadURL == "" {
		opt.UploadURL = opt.APIURL
	}

	httpClient := fshttp.NewClient(ctx)
	client := api.NewClient(opt.APIURL, opt.APIKey, httpClient)

	if _, err := client.GetAccount(ctx); err != nil {
		return nil, fmt.Errorf("flyfile: failed to validate credentials: %w", err)
	}

	f := &Fs{
		name:        name,
		root:        strings.Trim(root, "/"),
		opt:         *opt,
		client:      client,
		httpClient:  httpClient,
		folderCache: make(map[string]string),
	}
	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
		Move:                    f.Move,
		DirMove:                 f.DirMove,
		Purge:                   f.Purge,
	}).Fill(ctx, f)

	return f, nil
}

// Name returns the name of the remote
func (f *Fs) Name() string { return f.name }

// Root returns the root path
func (f *Fs) Root() string { return f.root }

// String returns a description of the Fs
func (f *Fs) String() string { return fmt.Sprintf("flyfile root '%s'", f.root) }

// Precision returns the precision of ModTime — not supported by FlyFile
func (f *Fs) Precision() time.Duration { return fs.ModTimeNotSupported }

// Hashes returns an empty hash set — FlyFile doesn't expose checksums
func (f *Fs) Hashes() hash.Set { return hash.Set(hash.None) }

// Features returns optional features
func (f *Fs) Features() *fs.Features { return f.features }

// folderID resolves a slash-separated path to a FlyFile folder ID.
// Creates intermediate folders as needed if create=true.
// Holds folderCacheMu for the entire walk to prevent concurrent goroutines
// from racing to create the same folder.
func (f *Fs) folderID(ctx context.Context, dirPath string, create bool) (string, error) {
	dirPath = strings.Trim(dirPath, "/")
	if dirPath == "" {
		return "", nil // root
	}

	f.folderCacheMu.Lock()
	defer f.folderCacheMu.Unlock()

	if time.Since(f.folderCacheAt) > 30*time.Second {
		f.folderCache = make(map[string]string)
	}
	if id, ok := f.folderCache[dirPath]; ok {
		return id, nil
	}

	parts := strings.Split(dirPath, "/")
	parentID := ""
	current := ""

	for _, part := range parts {
		if current == "" {
			current = part
		} else {
			current = current + "/" + part
		}

		if id, ok := f.folderCache[current]; ok {
			parentID = id
			continue
		}

		// Must unlock while doing network I/O to avoid deadlock on ctx cancel,
		// but re-check cache after re-lock.
		f.folderCacheMu.Unlock()
		folders, err := f.client.ListFolders(ctx, parentID)
		f.folderCacheMu.Lock()
		if err != nil {
			return "", err
		}

		// Re-check cache — another goroutine may have created it while unlocked
		if id, ok := f.folderCache[current]; ok {
			parentID = id
			continue
		}

		found := ""
		for _, folder := range folders {
			if folder.Name == part {
				found = folder.ID
				break
			}
		}

		if found == "" {
			if !create {
				return "", fs.ErrorDirNotFound
			}
			// Re-check cache one final time before creating — another goroutine may have
			// just created and cached this folder while we were doing ListFolders.
			if id, ok := f.folderCache[current]; ok {
				parentID = id
				continue
			}
			// Hold the lock during CreateFolder to prevent concurrent goroutines from
			// creating duplicate folders (SQL unique index doesn't cover NULL parentId).
			folder, err := f.client.CreateFolder(ctx, part, parentID)
			if err != nil {
				// Create failed — likely a race. Re-fetch to get the winner's folder.
				f.folderCacheMu.Unlock()
				foldersRetry, listErr := f.client.ListFolders(ctx, parentID)
				f.folderCacheMu.Lock()
				if listErr != nil {
					return "", err
				}
				for _, fld := range foldersRetry {
					if fld.Name == part {
						found = fld.ID
						break
					}
				}
				if found == "" {
					return "", err
				}
			} else {
				found = folder.ID
			}
		}

		f.folderCache[current] = found
		f.folderCacheAt = time.Now()
		parentID = found
	}

	return parentID, nil
}

// fullPath prepends the Fs root to a relative remote path
func (f *Fs) fullPath(remote string) string {
	if f.root == "" {
		return remote
	}
	if remote == "" {
		return f.root
	}
	return f.root + "/" + remote
}

// List returns the objects and subdirectories in dir
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	full := f.fullPath(dir)
	folderID, err := f.folderID(ctx, full, false)
	if err != nil {
		return nil, err
	}

	// List sub-folders
	folders, err := f.client.ListFolders(ctx, folderID)
	if err != nil {
		return nil, err
	}
	for _, folder := range folders {
		remote := path.Join(dir, folder.Name)
		entries = append(entries, fs.NewDir(remote, time.Time{}))

		f.folderCacheMu.Lock()
		f.folderCache[f.fullPath(remote)] = folder.ID
		f.folderCacheMu.Unlock()
	}

	// List files (all pages)
	page := 1
	limit := 100
	for {
		fileList, err := f.client.ListFiles(ctx, folderID, page, limit)
		if err != nil {
			return nil, err
		}
		for _, file := range fileList.Files {
			if file.Status == "UPLOADING" {
				continue // incomplete upload — treat as non-existent
			}
			sz, _ := strconv.ParseInt(file.Size, 10, 64)
			o := &Object{
				fs:      f,
				remote:  path.Join(dir, file.Name),
				size:    sz,
				modTime: file.Created,
				id:      file.ID,
			}
			entries = append(entries, o)
		}
		if fileList.CurrentPage >= fileList.TotalPages {
			break
		}
		page++
	}

	return entries, nil
}

// NewObject finds an existing file by remote path
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	full := f.fullPath(remote)
	dir, name := path.Split(full)
	dir = strings.TrimRight(dir, "/")

	folderID, err := f.folderID(ctx, dir, false)
	if err != nil {
		return nil, fs.ErrorObjectNotFound
	}

	page := 1
	limit := 100
	for {
		fileList, err := f.client.ListFiles(ctx, folderID, page, limit)
		if err != nil {
			return nil, err
		}
		for _, file := range fileList.Files {
			if file.Name == name && file.Status != "UPLOADING" {
				sz, _ := strconv.ParseInt(file.Size, 10, 64)
				return &Object{
					fs:      f,
					remote:  remote,
					size:    sz,
					modTime: file.Created,
					id:      file.ID,
				}, nil
			}
		}
		if fileList.CurrentPage >= fileList.TotalPages {
			break
		}
		page++
	}

	return nil, fs.ErrorObjectNotFound
}

// Put uploads a new file
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	remote := src.Remote()
	full := f.fullPath(remote)
	dir, name := path.Split(full)
	dir = strings.TrimRight(dir, "/")

	folderID, err := f.folderID(ctx, dir, true)
	if err != nil {
		return nil, fmt.Errorf("flyfile: mkdir for upload: %w", err)
	}

	workerURL, err := f.client.AssignUploader(ctx)
	if err != nil {
		return nil, fmt.Errorf("flyfile: assign uploader: %w", err)
	}

	initResp, err := f.client.UploadInit(ctx, name, folderID, src.Size())
	if err != nil {
		return nil, fmt.Errorf("flyfile: upload init: %w", err)
	}

	// If chunking or completion fails, clean up the orphaned File record.
	// Use a fresh context — the original may already be cancelled.
	completed := false
	defer func() {
		if !completed {
			abortCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			_ = f.client.AbortUpload(abortCtx, initResp.UploadID)
		}
	}()

	if err := f.uploadChunked(ctx, workerURL, initResp.UploadID, name, in, src.Size()); err != nil {
		return nil, fmt.Errorf("flyfile: upload: %w", err)
	}

	completeResp, err := f.client.UploadComplete(ctx, initResp.UploadID)
	if err != nil {
		return nil, fmt.Errorf("flyfile: upload complete: %w", err)
	}

	completed = true

	sz, _ := strconv.ParseInt(completeResp.File.Size, 10, 64)
	return &Object{
		fs:      f,
		remote:  remote,
		size:    sz,
		modTime: completeResp.File.Created,
		id:      completeResp.File.ID,
	}, nil
}

// Mkdir creates the directory (and parents) if they don't exist
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	_, err := f.folderID(ctx, f.fullPath(dir), true)
	return err
}

// Rmdir removes the directory; fails if non-empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	full := f.fullPath(dir)
	id, err := f.folderID(ctx, full, false)
	if err != nil {
		return err
	}
	if err := f.client.DeleteFolder(ctx, id); err != nil {
		return err
	}
	f.folderCacheMu.Lock()
	delete(f.folderCache, full)
	f.folderCacheMu.Unlock()
	return nil
}

// Purge removes the directory and all its contents
func (f *Fs) Purge(ctx context.Context, dir string) error {
	return f.Rmdir(ctx, dir)
}

// Move moves a file to a new location
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		return nil, fs.ErrorCantMove
	}

	dstFull := f.fullPath(remote)
	dstDir, dstName := path.Split(dstFull)
	dstDir = strings.TrimRight(dstDir, "/")

	srcFull := f.fullPath(src.Remote())
	srcDir, srcName := path.Split(srcFull)
	srcDir = strings.TrimRight(srcDir, "/")

	dstFolderID, err := f.folderID(ctx, dstDir, true)
	if err != nil {
		return nil, err
	}

	if srcDir != dstDir {
		if err := f.client.MoveFile(ctx, srcObj.id, dstFolderID); err != nil {
			return nil, err
		}
	}

	if srcName != dstName {
		if err := f.client.RenameFile(ctx, srcObj.id, dstName); err != nil {
			return nil, err
		}
	}

	return &Object{
		fs:      f,
		remote:  remote,
		size:    srcObj.size,
		modTime: srcObj.modTime,
		id:      srcObj.id,
	}, nil
}

// DirMove moves a directory to a new location
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	srcFs, ok := src.(*Fs)
	if !ok {
		return fs.ErrorCantDirMove
	}

	srcFull := srcFs.fullPath(srcRemote)
	dstFull := f.fullPath(dstRemote)

	srcID, err := srcFs.folderID(ctx, srcFull, false)
	if err != nil {
		return err
	}

	dstDir := path.Dir(dstFull)
	dstName := path.Base(dstFull)
	srcName := path.Base(srcFull)

	dstParentID, err := f.folderID(ctx, dstDir, true)
	if err != nil {
		return err
	}

	if dstDir != path.Dir(srcFull) {
		if err := f.client.MoveFolder(ctx, srcID, dstParentID); err != nil {
			return err
		}
	}

	if srcName != dstName {
		if err := f.client.RenameFolder(ctx, srcID, dstName); err != nil {
			return err
		}
	}

	f.folderCacheMu.Lock()
	f.folderCache = make(map[string]string)
	f.folderCacheMu.Unlock()

	return nil
}

// ------------------------------------------------------------
// Object methods

func (o *Object) String() string                                       { return o.remote }
func (o *Object) Remote() string                                       { return o.remote }
func (o *Object) ModTime(ctx context.Context) time.Time                { return o.modTime }
func (o *Object) Size() int64                                          { return o.size }
func (o *Object) Fs() fs.Info                                          { return o.fs }
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) { return "", hash.ErrUnsupported }
func (o *Object) Storable() bool                                        { return true }

// SetModTime is a no-op — FlyFile doesn't support setting mtime
func (o *Object) SetModTime(ctx context.Context, t time.Time) error {
	return fs.ErrorCantSetModTime
}

// Open returns an error — FlyFile is write-only
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	return nil, fmt.Errorf("flyfile: download not supported (write-only backend)")
}

// Update replaces the file contents by re-uploading
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	newObj, err := o.fs.Put(ctx, in, src, options...)
	if err != nil {
		return err
	}
	*o = *newObj.(*Object)
	return nil
}

// Remove deletes the file
func (o *Object) Remove(ctx context.Context) error {
	return o.fs.client.DeleteFile(ctx, o.id)
}

// Check interfaces
var (
	_ fs.Fs        = (*Fs)(nil)
	_ fs.Mover     = (*Fs)(nil)
	_ fs.DirMover  = (*Fs)(nil)
	_ fs.Purger    = (*Fs)(nil)
	_ fs.Object    = (*Object)(nil)
)
