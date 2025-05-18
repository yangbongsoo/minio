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

type MesureGetActiveInfo struct {
	Tag     string        `json:"tag"`
	Latency time.Duration `json:"latency"`
}

type RecordMultipartStart struct {
	UploadID  string    `json:"uploadID"`
	Bucket    string    `json:"bucket"`
	Object    string    `json:"object"`
	StartTime time.Time `json:"startTime"`
}

type MesurePutObjectPartElapsed struct {
	UploadID              string        `json:"uploadID"`
	Bucket                string        `json:"bucket"`
	Object                string        `json:"object"`
	PartID                int           `json:"partID"`
	EachPartUploadLatency time.Duration `json:"eachPartUploadLatency"`
}

type CompleteMultipartUploadLatency struct {
	UploadID     string    `json:"uploadID"`
	Bucket       string    `json:"bucket"`
	Object       string    `json:"object"`
	CompleteTime time.Time `json:"completeTime"`
}

type MultipartUploadLatency struct {
	operatorEndpoint string
	httpClient       *http.Client
}

func NewMultipartUploadLatency() *MultipartUploadLatency {
	return &MultipartUploadLatency{
		operatorEndpoint: "http://operator.minio-operator.svc.cluster.local:4221",
		httpClient:       &http.Client{Timeout: 5 * time.Second},
	}
}

func (m *MultipartUploadLatency) RecordMultipartStart(ctx context.Context, bucket, object, uploadID string, startTime time.Time) {
	logger.LogIf(ctx, "", fmt.Errorf("[YBS] RecordMultipartStart bucket: %s, object: %s, uploadID: %s", bucket, object, uploadID))
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		lockChan := make(chan struct{})
		go func() {
			err := m.sendRecordMultipartStart(bucket, object, uploadID, startTime)
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
			logger.LogIf(ctx, "", fmt.Errorf("[YBS] RecordMultipartStart timed out for uploadID: %s", uploadID))
		}
	}()
}

func (m *MultipartUploadLatency) MesureGetActiveInfo(ctx context.Context, tag string) func() {
	logger.LogIf(ctx, "", fmt.Errorf("[YBS] MesureGetActiveInfo"))
	before := time.Now()
	return func() {
		getActiveInfoLatency := time.Since(before)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			lockChan := make(chan struct{})
			go func() {
				err := m.sendMesureGetActiveInfo(tag, getActiveInfoLatency)
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
				logger.LogIf(ctx, "", fmt.Errorf("[YBS] MesureGetActiveInfo timed out for tag: %s", tag))
			}
		}()
	}
}

func (m *MultipartUploadLatency) MesurePutObjectPartElapsed(ctx context.Context, bucket, object, uploadID string, partID int) func() {
	logger.LogIf(ctx, "", fmt.Errorf("[YBS] MesurePutObjectPartElapsed bucket: %s, object: %s, uploadID: %s, partID: %d", bucket, object, uploadID, partID))
	before := time.Now()
	return func() {
		eachPartUploadLatency := time.Since(before)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			lockChan := make(chan struct{})
			go func() {
				err := m.sendMesurePutObjectPartElapsed(bucket, object, uploadID, partID, eachPartUploadLatency)
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
				logger.LogIf(ctx, "", fmt.Errorf("[YBS] MesurePutObjectPartElapsed timed out for uploadID: %s", uploadID))
			}
		}()
	}
}

func (m *MultipartUploadLatency) CompleteMultipartUploadLatency(ctx context.Context, bucket, object, uploadID string) func() {
	logger.LogIf(ctx, "", fmt.Errorf("[YBS] CompleteMultipartUploadLatency bucket: %s, object: %s, uploadID: %s", bucket, object, uploadID))
	return func() {
		completeTime := time.Now()
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			lockChan := make(chan struct{})
			go func() {
				err := m.sendCompleteMultipartUploadLatency(bucket, object, uploadID, completeTime)
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
				logger.LogIf(ctx, "", fmt.Errorf("[YBS] CompleteMultipartUploadLatency timed out for uploadID: %s", uploadID))
			}
		}()
	}
}

func (m *MultipartUploadLatency) sendMesureGetActiveInfo(tag string, getActiveInfoLatency time.Duration) error {
	report := MesureGetActiveInfo{
		Tag:     tag,
		Latency: getActiveInfoLatency,
	}
	logger.LogIf(context.Background(), "", fmt.Errorf("[YBS] send mesure get active info: %v", report))
	data, err := json.Marshal(report)
	if err != nil {
		logger.LogIf(context.Background(), "", fmt.Errorf("[YBS] failed to marshal latency report: %v", err))
		return err
	}
	return m.send(data, "/get-active-info-latency")
}

func (m *MultipartUploadLatency) sendRecordMultipartStart(bucket, object, uploadID string, startTime time.Time) error {
	report := RecordMultipartStart{
		UploadID:  uploadID,
		Bucket:    bucket,
		Object:    object,
		StartTime: startTime,
	}
	logger.LogIf(context.Background(), "", fmt.Errorf("[YBS] send record multipart start: %v", report))
	data, err := json.Marshal(report)
	if err != nil {
		logger.LogIf(context.Background(), "", fmt.Errorf("[YBS] failed to marshal latency report: %v", err))
		return err
	}
	return m.send(data, "/multipart-upload-latency-start")
}

func (m *MultipartUploadLatency) sendMesurePutObjectPartElapsed(bucket, object, uploadID string, partID int, eachPartUploadLatency time.Duration) error {
	report := MesurePutObjectPartElapsed{
		UploadID:              uploadID,
		Bucket:                bucket,
		Object:                object,
		PartID:                partID,
		EachPartUploadLatency: eachPartUploadLatency,
	}
	logger.LogIf(context.Background(), "", fmt.Errorf("[YBS] send mesure put object part elapsed: %v", report))
	data, err := json.Marshal(report)
	if err != nil {
		logger.LogIf(context.Background(), "", fmt.Errorf("[YBS] failed to marshal latency report: %v", err))
		return err
	}
	return m.send(data, "/multipart-upload-latency-part")
}

func (m *MultipartUploadLatency) sendCompleteMultipartUploadLatency(bucket, object, uploadID string, completeTime time.Time) error {
	report := CompleteMultipartUploadLatency{
		UploadID:     uploadID,
		Bucket:       bucket,
		Object:       object,
		CompleteTime: completeTime,
	}

	logger.LogIf(context.Background(), "", fmt.Errorf("[YBS] send multipart upload latency report: %v", report))
	data, err := json.Marshal(report)
	if err != nil {
		logger.LogIf(context.Background(), "", fmt.Errorf("[YBS] failed to marshal latency report: %v", err))
		return err
	}
	return m.send(data, "/multipart-upload-latency-complete")
}

func (m *MultipartUploadLatency) send(data []byte, path string) error {
	req, err := http.NewRequest("POST", m.operatorEndpoint+path, bytes.NewBuffer(data))
	if err != nil {
		logger.LogIf(context.Background(), "", fmt.Errorf("[YBS] failed to create HTTP request: %v", err))
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := m.httpClient.Do(req)
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
