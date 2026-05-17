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
func readChunk(src io.Reader) ([]byte, error) {
	buf := make([]byte, 0, chunkSize)
	tmp := make([]byte, 32*1024)
	for len(buf) < chunkSize {
		n, err := src.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}
		if err != nil {
			return buf, err
		}
	}
	return buf, nil
}

func (f *Fs) sendChunk(ctx context.Context, workerURL, uploadID, fileName string, index, total int, data []byte) error {
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
