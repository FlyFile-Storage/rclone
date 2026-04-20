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
	chunkSize     = 1024 * 1024 // 1 MB
	maxConcurrent = 4           // parallel chunk uploads
)

// uploadChunked reads src sequentially and uploads chunks in parallel (up to maxConcurrent).
func (f *Fs) uploadChunked(ctx context.Context, workerURL, uploadID, fileName string, src io.Reader, totalSize int64) error {
	var totalChunks int
	if totalSize > 0 {
		totalChunks = int((totalSize + chunkSize - 1) / chunkSize)
	}

	sem := make(chan struct{}, maxConcurrent)
	errCh := make(chan error, maxConcurrent)
	var wg sync.WaitGroup

	buf := make([]byte, chunkSize)
	chunkIndex := 0

	for {
		n, readErr := io.ReadFull(src, buf)
		if n == 0 {
			break
		}

		// Each goroutine needs its own copy of the data.
		data := make([]byte, n)
		copy(data, buf[:n])

		idx := chunkIndex
		tc := totalChunks

		sem <- struct{}{} // acquire slot — blocks if maxConcurrent already in flight
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := f.sendChunk(ctx, workerURL, uploadID, fileName, idx, tc, data); err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}()

		chunkIndex++

		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
		if readErr != nil {
			wg.Wait()
			return fmt.Errorf("flyfile read chunk %d: %w", chunkIndex, readErr)
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

func (f *Fs) sendChunk(ctx context.Context, workerURL, uploadID, fileName string, index, total int, data []byte) error {
	var body bytes.Buffer
	w := multipart.NewWriter(&body)

	_ = w.WriteField("uploadId", uploadID)
	_ = w.WriteField("chunkIndex", strconv.Itoa(index))
	_ = w.WriteField("totalChunks", strconv.Itoa(total))
	_ = w.WriteField("fileName", fileName)

	fw, err := w.CreateFormFile("chunk", fileName)
	if err != nil {
		return fmt.Errorf("flyfile chunk %d create form: %w", index, err)
	}
	if _, err := fw.Write(data); err != nil {
		return fmt.Errorf("flyfile chunk %d write: %w", index, err)
	}
	w.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, workerURL+"/upload-chunk", &body)
	if err != nil {
		return fmt.Errorf("flyfile chunk %d request: %w", index, err)
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	req.Header.Set("x-api-key", f.opt.APIKey)

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
