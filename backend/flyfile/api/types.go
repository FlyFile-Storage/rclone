// Package api provides types for the FlyFile API
package api

import "time"

// Error is returned by FlyFile API on failure
type Error struct {
	Message string `json:"message"`
	Status  int    `json:"status"`
}

func (e *Error) Error() string {
	return e.Message
}

// Account represents GET /account response
type Account struct {
	ID    string `json:"id"`
	Email string `json:"email"`
}

// File represents a file object from FlyFile API
type File struct {
	ID       string    `json:"id"`
	Name     string    `json:"name"`
	Size     string    `json:"size"` // API returns size as string
	FolderID string    `json:"folderId"`
	MimeType string    `json:"mimeType"`
	Status   string    `json:"status"`
	Created  time.Time `json:"createdAt"`
}

// Folder represents a folder object from FlyFile API
type Folder struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	ParentID string `json:"parentId"`
}

// FileList is the paginated response from GET /files
type FileList struct {
	Files       []File `json:"files"`
	TotalFiles  int    `json:"totalFiles"`
	TotalPages  int    `json:"totalPages"`
	CurrentPage int    `json:"currentPage"`
}

// FolderList is []Folder directly (API returns a bare array)
type FolderList = []Folder

// UploadInitResponse is the response from POST /upload/init
type UploadInitResponse struct {
	UploadID string `json:"uploadId"`
}

// UploadCompleteResponse is the response from POST /upload/complete
type UploadCompleteResponse struct {
	File File `json:"file"`
}

// RenameRequest is the body for PATCH /files/:id and PATCH /folders/:id
type RenameRequest struct {
	Name string `json:"name"`
}

// MoveFileRequest is the body for PATCH /files/:id/move
type MoveFileRequest struct {
	FolderID string `json:"folderId"`
}

// MoveFolderRequest is the body for PATCH /folders/:id/move
type MoveFolderRequest struct {
	ParentID string `json:"parentId"`
}

// CreateFolderRequest is the body for POST /folders
type CreateFolderRequest struct {
	Name     string `json:"name"`
	ParentID string `json:"parentId,omitempty"`
}

// BulkDeleteRequest is the body for POST /files/bulk-delete
type BulkDeleteRequest struct {
	IDs []string `json:"ids"`
}
