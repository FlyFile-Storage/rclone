package flyfile

import (
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"strconv"
	"sync"
)

const (
	chunkSize     = 19 * 1024 * 1024 // 19 MB, aligned with the uploader's Telegram block size
	maxConcurrent = 4                // parallel chunk uploads
)

type uploadChunk struct {
	index int
	path  string
	size  int64
}

// uploadChunked reads src sequentially, spools each chunk to disk, and uploads
// those temp files in parallel. This keeps RAM bounded while still keeping the
// network and uploader worker busy.
func (f *Fs) uploadChunked(ctx context.Context, workerURL, uploadID, fileName string, src io.Reader, totalSize int64) error {
	var totalChunks int
	if totalSize > 0 {
		totalChunks = int((totalSize + chunkSize - 1) / chunkSize)
	}

	sem := make(chan struct{}, maxConcurrent)
	errCh := make(chan error, maxConcurrent)
	var wg sync.WaitGroup
	chunkIndex := 0

	for {
		sem <- struct{}{} // limit both in-flight uploads and temp disk footprint

		chunk, readErr := spoolChunk(src, chunkIndex)
		if readErr != nil {
			<-sem
			wg.Wait()
			return fmt.Errorf("flyfile read chunk %d: %w", chunkIndex, readErr)
		}
		if chunk == nil {
			<-sem
			break
		}

		tc := totalChunks
		wg.Add(1)
		go func(chunk uploadChunk) {
			defer wg.Done()
			defer func() { <-sem }()
			defer func() { _ = os.Remove(chunk.path) }()

			if err := f.sendChunk(ctx, workerURL, uploadID, fileName, chunk.index, tc, chunk.path, chunk.size); err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}(*chunk)

		chunkIndex++
		if readErr == io.EOF {
			break
		}
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return nil
}

func spoolChunk(src io.Reader, index int) (*uploadChunk, error) {
	tmp, err := os.CreateTemp("", "rclone-flyfile-*")
	if err != nil {
		return nil, err
	}

	n, err := io.CopyN(tmp, src, chunkSize)
	closeErr := tmp.Close()
	if n == 0 {
		_ = os.Remove(tmp.Name())
		if err == io.EOF {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		return nil, closeErr
	}
	if closeErr != nil {
		_ = os.Remove(tmp.Name())
		return nil, closeErr
	}
	if err != nil && err != io.EOF {
		_ = os.Remove(tmp.Name())
		return nil, err
	}

	return &uploadChunk{index: index, path: tmp.Name(), size: n}, err
}

func (f *Fs) sendChunk(ctx context.Context, workerURL, uploadID, fileName string, index, total int, filePath string, fileSize int64) error {
	pr, pw := io.Pipe()
	w := multipart.NewWriter(pw)

	go func() {
		var err error
		defer func() {
			if err != nil {
				_ = pw.CloseWithError(err)
				return
			}
			_ = pw.Close()
		}()

		if err = w.WriteField("uploadId", uploadID); err != nil {
			return
		}
		if err = w.WriteField("chunkIndex", strconv.Itoa(index)); err != nil {
			return
		}
		if err = w.WriteField("totalChunks", strconv.Itoa(total)); err != nil {
			return
		}
		if err = w.WriteField("fileName", fileName); err != nil {
			return
		}

		fw, createErr := w.CreateFormFile("chunk", fileName)
		if createErr != nil {
			err = fmt.Errorf("flyfile chunk %d create form: %w", index, createErr)
			return
		}

		file, openErr := os.Open(filePath)
		if openErr != nil {
			err = fmt.Errorf("flyfile chunk %d open temp: %w", index, openErr)
			return
		}
		defer file.Close()

		if _, copyErr := io.Copy(fw, file); copyErr != nil {
			err = fmt.Errorf("flyfile chunk %d stream: %w", index, copyErr)
			return
		}
		err = w.Close()
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, workerURL+"/upload-chunk", pr)
	if err != nil {
		return fmt.Errorf("flyfile chunk %d request: %w", index, err)
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	req.Header.Set("x-api-key", f.opt.APIKey)
	req.ContentLength = multipartContentLength(w.Boundary(), uploadID, fileName, index, total, fileSize)

	resp, err := f.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("flyfile chunk %d send: %w", index, err)
	}
	resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("flyfile chunk %d: HTTP %d", index, resp.StatusCode)
	}

	return nil
}

func multipartContentLength(boundary, uploadID, fileName string, index, total int, fileSize int64) int64 {
	var n int64
	fields := []struct {
		name  string
		value string
	}{
		{"uploadId", uploadID},
		{"chunkIndex", strconv.Itoa(index)},
		{"totalChunks", strconv.Itoa(total)},
		{"fileName", fileName},
	}
	for _, field := range fields {
		n += int64(len("--" + boundary + "\r\n"))
		n += int64(len(`Content-Disposition: form-data; name="` + field.name + `"` + "\r\n\r\n"))
		n += int64(len(field.value + "\r\n"))
	}
	n += int64(len("--" + boundary + "\r\n"))
	n += int64(len(`Content-Disposition: form-data; name="chunk"; filename="` + fileName + `"` + "\r\n"))
	n += int64(len("Content-Type: application/octet-stream\r\n\r\n"))
	n += fileSize
	n += int64(len("\r\n"))
	n += int64(len("--" + boundary + "--\r\n"))
	return n
}
