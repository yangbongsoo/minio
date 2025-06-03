package stat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/minio/minio/internal/logger"
)

type MesureGetObjectFileInfo struct {
	Bucket  string        `json:"bucket"`
	Object  string        `json:"object"`
	Caller  string        `json:"caller"`
	Latency time.Duration `json:"latency"`
}

type MesureGetObjectWithFileInfo struct {
	Bucket  string        `json:"bucket"`
	Object  string        `json:"object"`
	Latency time.Duration `json:"latency"`
}

type MesureErasureDecodeEachPart struct {
	Bucket    string        `json:"bucket"`
	Object    string        `json:"object"`
	PartIndex int           `json:"partIndex"`
	Latency   time.Duration `json:"latency"`
}

type DownloadLatency struct {
	operatorEndpoint string
	httpClient       *http.Client
}

func NewDownloadLatency() *DownloadLatency {
	return &DownloadLatency{
		operatorEndpoint: "http://operator.minio-operator.svc.cluster.local:4221",
		httpClient:       &http.Client{Timeout: 5 * time.Second},
	}
}

func (d *DownloadLatency) MesureGetObjectFileInfo(ctx context.Context, bucket, object, caller string) func() {
	before := time.Now()
	return func() {
		getObjectFileInfoLatency := time.Since(before)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			lockChan := make(chan struct{})
			go func() {
				err := d.sendMesureGetObjectFileInfo(bucket, object, caller, getObjectFileInfoLatency)
				if err != nil {
					logger.LogIf(context.Background(), "", fmt.Errorf("[YBS] failed to send latency data: %v", err))
					close(lockChan)
					return
				}
				close(lockChan)
			}()

			select {
			case <-lockChan:
			case <-ctx.Done():
				fmt.Printf("[YBS_DOWNLOAD] MesureGetObjectFileInfo timed out for bucket: %s, object: %s\n", bucket, object)
			}
		}()
	}
}

func (d *DownloadLatency) MesureGetObjectWithFileInfo(ctx context.Context, bucket, object string) func() {
	before := time.Now()
	return func() {
		getObjectWithFileInfoLatency := time.Since(before)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			lockChan := make(chan struct{})
			go func() {
				err := d.sendMesureGetObjectWithFileInfo(bucket, object, getObjectWithFileInfoLatency)
				if err != nil {
					logger.LogIf(context.Background(), "", fmt.Errorf("[YBS] failed to send getObjectWithFileInfo latency data: %v", err))
					close(lockChan)
					return
				}
				close(lockChan)
			}()

			select {
			case <-lockChan:
			case <-ctx.Done():
				logger.LogIf(context.Background(), "", fmt.Errorf("[YBS] MesureGetObjectWithFileInfo timed out for bucket: %s, object: %s", bucket, object))
			}
		}()
	}
}

func (d *DownloadLatency) MesureErasureDecodeEachPart(ctx context.Context, bucket, object string, partIndex int, latency time.Duration) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		lockChan := make(chan struct{})
		go func() {
			err := d.sendMesureErasureDecodeEachPart(bucket, object, partIndex, latency)
			if err != nil {
				logger.LogIf(context.Background(), "", fmt.Errorf("[YBS] failed to send erasure decode part latency data: %v", err))
				close(lockChan)
				return
			}
			close(lockChan)
		}()

		select {
		case <-lockChan:
		case <-ctx.Done():
			logger.LogIf(context.Background(), "", fmt.Errorf("[YBS] MesureErasureDecodeEachPart timed out for bucket: %s, object: %s, part: %d", bucket, object, partIndex))
		}
	}()
}

func (d *DownloadLatency) sendMesureGetObjectWithFileInfo(bucket, object string, latency time.Duration) error {
	report := MesureGetObjectWithFileInfo{
		Bucket:  bucket,
		Object:  object,
		Latency: latency,
	}
	data, err := json.Marshal(report)
	if err != nil {
		logger.LogIf(context.Background(), "", fmt.Errorf("[YBS] failed to marshal getObjectWithFileInfo latency report: %v", err))
		return err
	}
	return d.send(data, "/get-object-with-file-info-latency")
}

func (d *DownloadLatency) sendMesureErasureDecodeEachPart(bucket, object string, partIndex int, latency time.Duration) error {
	report := MesureErasureDecodeEachPart{
		Bucket:    bucket,
		Object:    object,
		PartIndex: partIndex,
		Latency:   latency,
	}
	data, err := json.Marshal(report)
	if err != nil {
		logger.LogIf(context.Background(), "", fmt.Errorf("[YBS] failed to marshal erasure decode part latency report: %v", err))
		return err
	}
	return d.send(data, "/erasure-decode-each-part-latency")
}

func (d *DownloadLatency) sendMesureGetObjectFileInfo(bucket, object, caller string, latency time.Duration) error {
	report := MesureGetObjectFileInfo{
		Bucket:  bucket,
		Object:  object,
		Latency: latency,
		Caller:  caller,
	}
	data, err := json.Marshal(report)
	if err != nil {
		logger.LogIf(context.Background(), "", fmt.Errorf("[YBS] failed to marshal latency report: %v", err))
		return err
	}
	return d.send(data, "/get-object-file-info-latency")
}

func (d *DownloadLatency) send(data []byte, path string) error {
	req, err := http.NewRequest("POST", d.operatorEndpoint+path, bytes.NewBuffer(data))
	if err != nil {
		logger.LogIf(context.Background(), "", fmt.Errorf("[YBS] failed to create HTTP request: %v", err))
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		logger.LogIf(context.Background(), "", fmt.Errorf("[YBS] failed to send latency data: %v", err))
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		logger.LogIf(context.Background(), "", fmt.Errorf("[YBS] operator returned non-success status: %d", resp.StatusCode))
		return fmt.Errorf("operator returned non-success status: %d", resp.StatusCode)
	}

	logger.LogIf(context.Background(), "", fmt.Errorf("[YBS] send latency data success"))
	return nil
}
