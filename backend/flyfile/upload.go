package flyfile

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const (
	chunkSize     = 10 * 1024 * 1024 // 10 MiB; uploader compacts into Telegram-sized blocks
	maxConcurrent = 8
)

// uploadChunked reads src sequentially, buffers each chunk in RAM, and uploads
// chunks in parallel bounded by maxConcurrent. At most maxConcurrent×chunkSize
// (~80 MiB) lives in memory at once.
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
		sem <- struct{}{} // acquire slot before reading to bound in-flight memory

		buf, readErr := readChunk(src)
		if readErr != nil && readErr != io.EOF {
			<-sem
			wg.Wait()
			return fmt.Errorf("flyfile read chunk %d: %w", chunkIndex, readErr)
		}
		if len(buf) == 0 {
			<-sem
			break
		}

		idx := chunkIndex
		tc := totalChunks
		wg.Add(1)
		go func(idx int, buf []byte) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := f.sendChunk(ctx, workerURL, uploadID, fileName, idx, tc, buf); err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}(idx, buf)

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

// readChunk reads up to chunkSize bytes from src into a new buffer.
// Returns (buf, io.EOF) when src is exhausted (buf may be non-empty on that call).
// Never overshoots chunkSize — the read buffer is sized to fit the remaining slot.
func readChunk(src io.Reader) ([]byte, error) {
	buf := make([]byte, 0, chunkSize)
	tmp := make([]byte, 32*1024)
	for len(buf) < chunkSize {
		remaining := chunkSize - len(buf)
		readSize := len(tmp)
		if readSize > remaining {
			readSize = remaining
		}
		n, err := src.Read(tmp[:readSize])
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}
		if err != nil {
			return buf, err
		}
	}
	return buf, nil
}

const (
	sendChunkMaxAttempts = 5
	sendChunkBaseDelay   = time.Second
	sendChunkMaxDelay    = 30 * time.Second
)

// sendChunk POSTs one chunk to the uploader worker with retry on transient failures.
// Retries are needed because:
//   - The uploader may briefly 5xx during chunk packing or Telegram dispatch.
//   - Nginx / load balancers may close idle connections mid-upload (EOF / connection reset).
//   - 429 rate-limit responses need to back off, not abort.
//
// Without retry, a single transient error aborts the entire rclone upload — rclone
// then re-uploads the file from scratch via its outer retry, multiplying total time.
func (f *Fs) sendChunk(ctx context.Context, workerURL, uploadID, fileName string, index, total int, data []byte) error {
	var lastErr error
	for attempt := 1; attempt <= sendChunkMaxAttempts; attempt++ {
		err := f.sendChunkOnce(ctx, workerURL, uploadID, fileName, index, total, data)
		if err == nil {
			return nil
		}
		lastErr = err

		// Don't retry if context is dead.
		if ctx.Err() != nil {
			return fmt.Errorf("flyfile chunk %d cancelled: %w", index, ctx.Err())
		}

		// Don't retry on permanent client errors (4xx except 408/429).
		if isPermanentHTTPError(err) {
			return err
		}

		if attempt == sendChunkMaxAttempts {
			break
		}

		// Exponential backoff capped at sendChunkMaxDelay.
		delay := sendChunkBaseDelay << (attempt - 1)
		if delay > sendChunkMaxDelay {
			delay = sendChunkMaxDelay
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return fmt.Errorf("flyfile chunk %d cancelled during backoff: %w", index, ctx.Err())
		}
	}
	return fmt.Errorf("flyfile chunk %d failed after %d attempts: %w", index, sendChunkMaxAttempts, lastErr)
}

// sendChunkHTTPError carries the HTTP status so the retry layer can decide.
type sendChunkHTTPError struct {
	chunk      int
	statusCode int
}

func (e *sendChunkHTTPError) Error() string {
	return fmt.Sprintf("flyfile chunk %d: HTTP %d", e.chunk, e.statusCode)
}

// isPermanentHTTPError reports whether err is a 4xx (other than 408/429) — these
// won't be fixed by retrying.
func isPermanentHTTPError(err error) bool {
	httpErr, ok := err.(*sendChunkHTTPError)
	if !ok {
		return false
	}
	if httpErr.statusCode == 408 || httpErr.statusCode == 429 {
		return false
	}
	return httpErr.statusCode >= 400 && httpErr.statusCode < 500
}

// sendChunkOnce performs a single attempt. The multipart body is re-created here
// each call because the io.Pipe reader is single-use.
func (f *Fs) sendChunkOnce(ctx context.Context, workerURL, uploadID, fileName string, index, total int, data []byte) error {
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

		if _, copyErr := io.Copy(fw, bytes.NewReader(data)); copyErr != nil {
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
	req.ContentLength = multipartContentLength(w.Boundary(), uploadID, fileName, index, total, int64(len(data)))

	resp, err := f.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("flyfile chunk %d send: %w", index, err)
	}
	resp.Body.Close()

	if resp.StatusCode >= 400 {
		return &sendChunkHTTPError{chunk: index, statusCode: resp.StatusCode}
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
