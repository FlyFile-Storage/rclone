// Package api provides the HTTP client for the FlyFile API
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// Client wraps HTTP calls to the FlyFile API
type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

// NewClient creates a new FlyFile API client
func NewClient(baseURL, apiKey string, httpClient *http.Client) *Client {
	return &Client{
		baseURL:    baseURL,
		apiKey:     apiKey,
		httpClient: httpClient,
	}
}

func (c *Client) newRequest(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("x-api-key", c.apiKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req, nil
}

func (c *Client) do(req *http.Request, out interface{}) error {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		var apiErr Error
		if jsonErr := json.NewDecoder(resp.Body).Decode(&apiErr); jsonErr == nil && apiErr.Message != "" {
			apiErr.Status = resp.StatusCode
			return &apiErr
		}
		return fmt.Errorf("flyfile: HTTP %d", resp.StatusCode)
	}

	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func (c *Client) doJSON(ctx context.Context, method, path string, in, out interface{}) error {
	var body io.Reader
	if in != nil {
		data, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	req, err := c.newRequest(ctx, method, path, body)
	if err != nil {
		return err
	}
	return c.do(req, out)
}

// GetAccount validates credentials by fetching account info
func (c *Client) GetAccount(ctx context.Context) (*Account, error) {
	var out Account
	return &out, c.doJSON(ctx, http.MethodGet, "/api/v1/account", nil, &out)
}

// ListFolders returns folders under parentId (empty = root)
func (c *Client) ListFolders(ctx context.Context, parentID string) (FolderList, error) {
	p := "/api/v1/folders"
	if parentID != "" {
		p += "?parentId=" + parentID
	}
	var out FolderList
	return out, c.doJSON(ctx, http.MethodGet, p, nil, &out)
}

// CreateFolder creates a folder and returns it
func (c *Client) CreateFolder(ctx context.Context, name, parentID string) (*Folder, error) {
	in := CreateFolderRequest{Name: name, ParentID: parentID}
	var out Folder
	return &out, c.doJSON(ctx, http.MethodPost, "/api/v1/folders", in, &out)
}

// RenameFolder renames a folder
func (c *Client) RenameFolder(ctx context.Context, id, name string) error {
	return c.doJSON(ctx, http.MethodPatch, "/api/v1/folders/"+id, RenameRequest{Name: name}, nil)
}

// MoveFolder moves a folder to a new parent
func (c *Client) MoveFolder(ctx context.Context, id, parentID string) error {
	return c.doJSON(ctx, http.MethodPatch, "/api/v1/folders/"+id+"/move", MoveFolderRequest{ParentID: parentID}, nil)
}

// DeleteFolder deletes a folder
func (c *Client) DeleteFolder(ctx context.Context, id string) error {
	return c.doJSON(ctx, http.MethodDelete, "/api/v1/folders/"+id, nil, nil)
}

// ListFiles returns paginated files in a folder
func (c *Client) ListFiles(ctx context.Context, folderID string, page, limit int) (*FileList, error) {
	p := fmt.Sprintf("/api/v1/files?folderId=%s&page=%d&limit=%d", folderID, page, limit)
	var out FileList
	return &out, c.doJSON(ctx, http.MethodGet, p, nil, &out)
}

// RenameFile renames a file
func (c *Client) RenameFile(ctx context.Context, id, name string) error {
	return c.doJSON(ctx, http.MethodPatch, "/api/v1/files/"+id, RenameRequest{Name: name}, nil)
}

// MoveFile moves a file to a different folder
func (c *Client) MoveFile(ctx context.Context, id, folderID string) error {
	return c.doJSON(ctx, http.MethodPatch, "/api/v1/files/"+id+"/move", MoveFileRequest{FolderID: folderID}, nil)
}

// DeleteFile deletes a file
func (c *Client) DeleteFile(ctx context.Context, id string) error {
	return c.doJSON(ctx, http.MethodDelete, "/api/v1/files/"+id, nil, nil)
}

// AssignUploader returns the URL of the best available uploader worker
func (c *Client) AssignUploader(ctx context.Context) (string, error) {
	var out struct {
		URL string `json:"url"`
	}
	return out.URL, c.doJSON(ctx, http.MethodGet, "/api/v1/upload/assign", nil, &out)
}

// UploadInit starts an upload session and returns the uploadId
func (c *Client) UploadInit(ctx context.Context, name, folderID string, size int64) (*UploadInitResponse, error) {
	body := map[string]interface{}{
		"fileName": name,
		"fileSize": size, // int64 serializes as JSON number — backend accepts both JSON and multipart
		"folderId": folderID,
	}
	var out UploadInitResponse
	return &out, c.doJSON(ctx, http.MethodPost, "/api/v1/upload/init", body, &out)
}

// UploadComplete finalizes an upload
func (c *Client) UploadComplete(ctx context.Context, uploadID string) (*UploadCompleteResponse, error) {
	body := map[string]string{"uploadId": uploadID}
	var out UploadCompleteResponse
	return &out, c.doJSON(ctx, http.MethodPost, "/api/v1/upload/complete", body, &out)
}

// AbortUpload deletes an incomplete upload (status=UPLOADING) and its chunks
func (c *Client) AbortUpload(ctx context.Context, uploadID string) error {
	return c.doJSON(ctx, http.MethodDelete, "/api/v1/upload/"+uploadID, nil, nil)
}
