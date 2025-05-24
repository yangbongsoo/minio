// Copyright (c) 2015-2023 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/readahead"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/config/storageclass"
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/hash"
	xhttp "github.com/minio/minio/internal/http"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/mimedb"
	"github.com/minio/pkg/v3/sync/errgroup"
	"github.com/minio/sio"
)

func (er erasureObjects) getUploadIDDir(bucket, object, uploadID string) string {
	uploadUUID := uploadID
	uploadBytes, err := base64.RawURLEncoding.DecodeString(uploadID)
	if err == nil {
		slc := strings.SplitN(string(uploadBytes), ".", 2)
		if len(slc) == 2 {
			uploadUUID = slc[1]
		}
	}
	return pathJoin(er.getMultipartSHADir(bucket, object), uploadUUID)
}

func (er erasureObjects) getMultipartSHADir(bucket, object string) string {
	return getSHA256Hash([]byte(pathJoin(bucket, object)))
}

// checkUploadIDExists - verify if a given uploadID exists and is valid.
func (er erasureObjects) checkUploadIDExists(ctx context.Context, bucket, object, uploadID string, write bool) (fi FileInfo, metArr []FileInfo, activeDisks []StorageAPI, err error) {
	// <<< 로그 추가 지점 1 >>>
	logger.LogIf(ctx, "", fmt.Errorf("[YBS_DEBUG] checkUploadIDExists CALLED: bucket=%s, object=%s, uploadID=%s, write=%t", bucket, object, uploadID, write))

	defer multipartLatency.MesureCheckUploadIDExists(ctx, bucket, object, uploadID, time.Now())()
	defer func() {
		if errors.Is(err, errFileNotFound) {
			err = errUploadIDNotFound
		}
		// <<< 로그 추가 지점 2 >>>
		logger.LogIf(ctx, "", fmt.Errorf("[YBS_DEBUG] checkUploadIDExists RETURNING: bucket=%s, object=%s, uploadID=%s, write=%t, returning_err=%v", bucket, object, uploadID, write, err))
		if err == nil && fi.IsValid() {
			logger.LogIf(ctx, "", fmt.Errorf("[YBS_DEBUG] checkUploadIDExists RETURNING FileInfo: Name=%s, Size=%d, Distribution=%v, Metadata=%v", fi.Name, fi.Size, fi.Erasure.Distribution, fi.Metadata))
		} else if err == nil {
			logger.LogIf(ctx, "", fmt.Errorf("[YBS_DEBUG] checkUploadIDExists RETURNING FileInfo IS INVALID (err is nil). fi.Name=%s, fi.Size=%d, fi.Erasure.Distribution=%v", fi.Name, fi.Size, fi.Erasure.Distribution))
		}
	}()

	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)
	// <<< 로그 추가 지점 3 >>>
	logger.LogIf(ctx, "", fmt.Errorf("[YBS_DEBUG] checkUploadIDExists: uploadIDPath=%s, write=%t", uploadIDPath, write))

	// logger.LogIf(ctx, "", fmt.Errorf("[YBS] checkUploadIDExists 호출")) // 기존 로그
	currentActiveDisks, _, activeIDCCount := er.GetActiveInfo(ctx, er.getDisks(), "checkUploadIDExists")
	// activeDisks 변수를 여기서 초기화합니다. currentActiveDisks는 임시 변수로 사용합니다.
	activeDisks = currentActiveDisks
	// DecideErasureCodingParameter는 activeDisks를 포함하여 4개의 값을 반환합니다.
	var dataDrives, parityDrives int
	var returnFlag bool
	activeDisks, dataDrives, parityDrives, returnFlag = er.DecideErasureCodingParameter(ctx, activeDisks, activeIDCCount)
	// <<< 로그 추가 지점 4 >>>
	logger.LogIf(ctx, "", fmt.Errorf("[YBS_DEBUG] checkUploadIDExists: After DecideErasureCodingParameter. Initial activeDisks count=%d, dataDrives=%d, parityDrives=%d, activeIDCCount=%d, returnFlag=%t, write=%t", len(activeDisks), dataDrives, parityDrives, activeIDCCount, returnFlag, write))

	partsMetadata, errs := readAllFileInfo(ctx, activeDisks, bucket, minioMetaMultipartBucket,
		uploadIDPath, "", false, false)
	// <<< 로그 추가 지점 5 >>>
	validFIs := 0
	var firstValidFIDistribution []int
	var firstValidFIName string
	for _, pm := range partsMetadata {
		if pm.IsValid() {
			validFIs++
			if len(pm.Erasure.Distribution) > 0 && len(firstValidFIDistribution) == 0 {
				firstValidFIDistribution = pm.Erasure.Distribution
				firstValidFIName = pm.Name
			}
		}
	}
	logger.LogIf(ctx, "", fmt.Errorf("[YBS_DEBUG] checkUploadIDExists: After first readAllFileInfo. uploadIDPath=%s, validFIs=%d, firstValidFIDistribution (Name: %s, Dist: %v), write=%t", uploadIDPath, validFIs, firstValidFIName, firstValidFIDistribution, write))

	var foundValidFI *FileInfo
	for i := range partsMetadata {
		if partsMetadata[i].IsValid() && len(partsMetadata[i].Erasure.Distribution) > 0 {
			foundValidFI = &partsMetadata[i]
			break
		}
	}

	if foundValidFI != nil {
		// logger.LogIf(ctx, "", fmt.Errorf("[YBS] checkUploadIDExists foundValidFI: %v", foundValidFI)) // 기존 로그, 아래 상세 로그로 대체 가능
		// <<< 로그 추가 지점 6 >>>
		logger.LogIf(ctx, "", fmt.Errorf("[YBS_DEBUG] checkUploadIDExists: foundValidFI.Name=%s, Distribution=%v, Metadata=%v, write=%t", foundValidFI.Name, foundValidFI.Erasure.Distribution, foundValidFI.Metadata, write))

		if len(activeDisks) == len(foundValidFI.Erasure.Distribution) {
			// <<< 로그 추가 지점 7 (최적화 경로 진입) >>>
			logger.LogIf(ctx, "", fmt.Errorf("[YBS_DEBUG] checkUploadIDExists: Entering OPTIMIZED metadata reorder path. write=%t", write))
			// logger.LogIf(ctx, "", fmt.Errorf("[YBS] checkUploadIDExists shuffleDisks and reorder metadata (no second readAllFileInfo)")) // 기존 로그

			originalPartsMetadataForShuffle := make([]FileInfo, len(partsMetadata))
			copy(originalPartsMetadataForShuffle, partsMetadata)
			originalErrsForShuffle := make([]error, len(errs))
			copy(originalErrsForShuffle, errs)

			activeDisks = shuffleDisks(activeDisks, foundValidFI.Erasure.Distribution)

			newPartsMetadata := make([]FileInfo, len(originalPartsMetadataForShuffle))
			newErrs := make([]error, len(originalErrsForShuffle))
			distribution := foundValidFI.Erasure.Distribution

			for k := 0; k < len(originalPartsMetadataForShuffle); k++ {
				destIndex := distribution[k] - 1
				if destIndex >= 0 && destIndex < len(newPartsMetadata) {
					newPartsMetadata[destIndex] = originalPartsMetadataForShuffle[k]
					newErrs[destIndex] = originalErrsForShuffle[k]
				} else {
					logger.LogIf(ctx, "", fmt.Errorf("[YBS_DEBUG] internal error: shuffle distribution index %d (from original index k=%d with value distribution[k]=%d) out of bounds for metadata length %d, write=%t", destIndex, k, distribution[k], len(newPartsMetadata), write))
					err = fmt.Errorf("internal error during metadata shuffle: distribution index %d out of bounds (original index k=%d, distribution[k]=%d)", destIndex, k, distribution[k])
					return // fi, partsMetadata (metArr), activeDisks, err
				}
			}
			partsMetadata = newPartsMetadata
			errs = newErrs
			dataDrives = foundValidFI.Erasure.DataBlocks
			parityDrives = foundValidFI.Erasure.ParityBlocks
		} else {
			// <<< 로그 추가 지점 8 (최적화 경로 미진입 - 길이 불일치) >>>
			logger.LogIf(ctx, "", fmt.Errorf("[YBS_DEBUG] checkUploadIDExists: SKIPPED optimized path. Reason: len(activeDisks)=%d != len(foundValidFI.Erasure.Distribution)=%d. write=%t", len(activeDisks), len(foundValidFI.Erasure.Distribution), write))
			// activeDisks는 이미 currentActiveDisks의 값으로 설정되어 있으므로 변경 필요 없음
		}
	} else {
		// <<< 로그 추가 지점 9 (최적화 경로 미진입 - foundValidFI is nil) >>>
		logger.LogIf(ctx, "", fmt.Errorf("[YBS_DEBUG] checkUploadIDExists: SKIPPED optimized path. Reason: foundValidFI is nil. write=%t", write))
		// activeDisks는 이미 currentActiveDisks의 값으로 설정되어 있으므로 변경 필요 없음
	}

	readQuorum := dataDrives
	writeQuorum := dataDrives
	// _ = parityDrives // 주석 처리된 변수는 그대로 둡니다.

	// err 변수는 여기서 초기화되지 않고, 이전 로직(distribution index out of bounds)에서 설정되었을 수 있습니다.
	// 이 부분을 명확히 하기 위해, 아래의 반환 전에 err이 설정되지 않았다면 nil이라고 가정합니다.
	// 그러나 Go에서는 명명된 반환값을 사용하므로, err은 이미 특정 값을 가질 수 있습니다.
	// 여기서는 추가적인 err 할당 없이 진행합니다.

	if readQuorum < 0 {
		// <<< 로그 추가 지점 10 >>>
		logger.LogIf(ctx, "", fmt.Errorf("[YBS_DEBUG] checkUploadIDExists: readQuorum < 0 (%d), write=%t", readQuorum, write))
		err = errErasureReadQuorum // 명시적으로 err 설정
		return fi, partsMetadata, activeDisks, err
	}

	if writeQuorum < 0 {
		// <<< 로그 추가 지점 11 >>>
		logger.LogIf(ctx, "", fmt.Errorf("[YBS_DEBUG] checkUploadIDExists: writeQuorum < 0 (%d), write=%t", writeQuorum, write))
		err = errErasureWriteQuorum // 명시적으로 err 설정
		return fi, partsMetadata, activeDisks, err
	}

	quorum := readQuorum
	if write {
		quorum = writeQuorum
	}
	// <<< 로그 추가 지점 12 >>>
	logger.LogIf(ctx, "", fmt.Errorf("[YBS_DEBUG] checkUploadIDExists: quorum=%d (write=%t)", quorum, write))

	_, modTime, etag := listOnlineDisks(activeDisks, partsMetadata, errs, quorum)
	// <<< 로그 추가 지점 13 >>>
	validPMs := 0
	for _, pm := range partsMetadata {
		if pm.IsValid() {
			validPMs++
		}
	}
	logger.LogIf(ctx, "", fmt.Errorf("[YBS_DEBUG] checkUploadIDExists: Before pickValidFileInfo. activeDisks_len=%d, partsMetadata_valid_count=%d, quorum=%d, write=%t", len(activeDisks), validPMs, quorum, write))

	var reduceErr error
	if write {
		reduceErr = reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, writeQuorum)
	} else {
		reduceErr = reduceReadQuorumErrs(ctx, errs, objectOpIgnoredErrs, readQuorum)
	}
	if reduceErr != nil {
		// <<< 로그 추가 지점 14 >>>
		logger.LogIf(ctx, "", fmt.Errorf("[YBS_DEBUG] checkUploadIDExists: reduceQuorumErrs failed: %v (write=%t)", reduceErr, write))
		err = reduceErr // 명시적으로 err 설정
		return fi, partsMetadata, activeDisks, err
	}

	fi, err = pickValidFileInfo(ctx, partsMetadata, modTime, etag, quorum)
	// <<< 로그 추가 지점 15 >>>
	if err != nil {
		logger.LogIf(ctx, "", fmt.Errorf("[YBS_DEBUG] checkUploadIDExists: pickValidFileInfo failed: %v, write=%t", err, write))
	} else if !fi.IsValid() {
		logger.LogIf(ctx, "", fmt.Errorf("[YBS_DEBUG] checkUploadIDExists: pickValidFileInfo returned INVALID FileInfo (err is nil). fi.Name=%s, fi.Size=%d, fi.Erasure.Distribution=%v, write=%t", fi.Name, fi.Size, fi.Erasure.Distribution, write))
	}
	// err은 pickValidFileInfo에서 설정된 값을 그대로 사용
	return fi, partsMetadata, activeDisks, err
}

func (er erasureObjects) checkUploadIDExistsOriginal(ctx context.Context, bucket, object, uploadID string, write bool) (fi FileInfo, metArr []FileInfo, err error) {
	defer func() {
		if errors.Is(err, errFileNotFound) {
			err = errUploadIDNotFound
		}
	}()

	// 초기화: 업로드 ID 디렉토리 경로 계산
	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)

	// 디스크 접근: 모든 디스크 가져오기
	storageDisks := er.getDisks()

	// 메타데이터 읽기: 모든 디스크에서 동시에 메타데이터 파일 읽기
	// Read metadata associated with the object from all disks.
	partsMetadata, errs := readAllFileInfo(ctx, storageDisks, bucket, minioMetaMultipartBucket,
		uploadIDPath, "", false, false)

	// 쿼럼 계산: 읽기/쓰기 연산에 필요한 쿼럼 결정
	readQuorum, writeQuorum, err := objectQuorumFromMeta(ctx, partsMetadata, errs, er.defaultParityCount)
	if err != nil {
		return fi, nil, err
	}

	if readQuorum < 0 {
		return fi, nil, errErasureReadQuorum
	}

	if writeQuorum < 0 {
		return fi, nil, errErasureWriteQuorum
	}

	quorum := readQuorum
	if write {
		quorum = writeQuorum
	}

	// 온라인 디스크 확인: 사용 가능한 디스크 파악 및 메타데이터 일관성 확인
	// List all online disks.
	_, modTime, etag := listOnlineDisks(storageDisks, partsMetadata, errs, quorum)

	if write {
		err = reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, writeQuorum)
	} else {
		err = reduceReadQuorumErrs(ctx, errs, objectOpIgnoredErrs, readQuorum)
	}
	if err != nil {
		return fi, nil, err
	}

	// 메타데이터 선택: 일관된 메타데이터 중 하나 선택
	// Pick one from the first valid metadata.
	fi, err = pickValidFileInfo(ctx, partsMetadata, modTime, etag, quorum)
	return fi, partsMetadata, err
}

// cleanupMultipartPath removes all extraneous files and parts from the multipart folder, this is used per CompleteMultipart.
// do not use this function outside of completeMultipartUpload()
func (er erasureObjects) cleanupMultipartPath(ctx context.Context, activeDisks []StorageAPI, paths ...string) {
	logger.LogIf(ctx, "", fmt.Errorf("[YBS] cleanupMultipartPath: %v", paths))
	// storageDisks := er.getDisks()
	// activeDisks, _, activeIDCCount := er.GetActiveInfo(ctx, er.getDisks())
	if len(activeDisks) == 0 {
		activeDisks, _, activeIDCCount := er.GetActiveInfo(ctx, er.getDisks(), "cleanupMultipartPath")
		logger.LogIf(ctx, "", fmt.Errorf("[YBS] cleanupMultipartPath activeDisks: %v, activeIDCCount: %v", activeDisks, activeIDCCount))
	}

	g := errgroup.WithNErrs(len(activeDisks))
	for index, disk := range activeDisks {
		if disk == nil {
			continue
		}
		index := index
		g.Go(func() error {
			_ = activeDisks[index].DeleteBulk(ctx, minioMetaMultipartBucket, paths...)
			return nil
		}, index)
	}
	g.Wait()
}

// Clean-up the old multipart uploads. Should be run in a Go routine.
func (er erasureObjects) cleanupStaleUploads(ctx context.Context) {
	// run multiple cleanup's local to this server.
	var wg sync.WaitGroup
	for _, disk := range er.getLocalDisks() {
		if disk != nil {
			wg.Add(1)
			go func(disk StorageAPI) {
				defer wg.Done()
				er.cleanupStaleUploadsOnDisk(ctx, disk)
			}(disk)
		}
	}
	wg.Wait()
}

func (er erasureObjects) deleteAll(ctx context.Context, bucket, prefix string) {
	var wg sync.WaitGroup
	for _, disk := range er.getDisks() {
		if disk == nil {
			continue
		}
		wg.Add(1)
		go func(disk StorageAPI) {
			defer wg.Done()
			disk.Delete(ctx, bucket, prefix, DeleteOptions{
				Recursive: true,
				Immediate: false,
			})
		}(disk)
	}
	wg.Wait()
}

// Remove the old multipart uploads on the given disk.
func (er erasureObjects) cleanupStaleUploadsOnDisk(ctx context.Context, disk StorageAPI) {
	drivePath := disk.Endpoint().Path

	readDirFn(pathJoin(drivePath, minioMetaMultipartBucket), func(shaDir string, typ os.FileMode) error {
		readDirFn(pathJoin(drivePath, minioMetaMultipartBucket, shaDir), func(uploadIDDir string, typ os.FileMode) error {
			uploadIDPath := pathJoin(shaDir, uploadIDDir)
			var modTime time.Time
			// Upload IDs are of the form base64_url(<UUID>x<UnixNano>), we can extract the time from the UUID.
			if b64, err := base64.RawURLEncoding.DecodeString(uploadIDDir); err == nil {
				if split := strings.Split(string(b64), "x"); len(split) == 2 {
					t, err := strconv.ParseInt(split[1], 10, 64)
					if err == nil {
						modTime = time.Unix(0, t)
					}
				}
			}
			// Fallback for older uploads without time in the ID.
			if modTime.IsZero() {
				wait := deleteMultipartCleanupSleeper.Timer(ctx)
				fi, err := disk.ReadVersion(ctx, "", minioMetaMultipartBucket, uploadIDPath, "", ReadOptions{})
				if err != nil {
					return nil
				}
				modTime = fi.ModTime
				wait()
			}
			if time.Since(modTime) < globalAPIConfig.getStaleUploadsExpiry() {
				return nil
			}
			w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
			return w.Run(func() error {
				wait := deleteMultipartCleanupSleeper.Timer(ctx)
				pathUUID := mustGetUUID()
				targetPath := pathJoin(drivePath, minioMetaTmpDeletedBucket, pathUUID)
				renameAll(pathJoin(drivePath, minioMetaMultipartBucket, uploadIDPath), targetPath, pathJoin(drivePath, minioMetaBucket))
				wait()
				return nil
			})
		})
		// Get the modtime of the shaDir.
		vi, err := disk.StatVol(ctx, pathJoin(minioMetaMultipartBucket, shaDir))
		if err != nil {
			return nil
		}
		// Modtime is returned in the Created field. See (*xlStorage).StatVol
		if time.Since(vi.Created) < globalAPIConfig.getStaleUploadsExpiry() {
			return nil
		}
		w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
		return w.Run(func() error {
			wait := deleteMultipartCleanupSleeper.Timer(ctx)
			pathUUID := mustGetUUID()
			targetPath := pathJoin(drivePath, minioMetaTmpDeletedBucket, pathUUID)

			// We are not deleting shaDir recursively here, if shaDir is empty
			// and its older then we can happily delete it.
			Rename(pathJoin(drivePath, minioMetaMultipartBucket, shaDir), targetPath)
			wait()
			return nil
		})
	})

	readDirFn(pathJoin(drivePath, minioMetaTmpBucket), func(tmpDir string, typ os.FileMode) error {
		if strings.HasPrefix(tmpDir, ".trash") {
			// do not remove .trash/ here, it has its own routines
			return nil
		}
		vi, err := disk.StatVol(ctx, pathJoin(minioMetaTmpBucket, tmpDir))
		if err != nil {
			return nil
		}
		w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
		return w.Run(func() error {
			wait := deleteMultipartCleanupSleeper.Timer(ctx)
			if time.Since(vi.Created) > globalAPIConfig.getStaleUploadsExpiry() {
				pathUUID := mustGetUUID()
				targetPath := pathJoin(drivePath, minioMetaTmpDeletedBucket, pathUUID)

				renameAll(pathJoin(drivePath, minioMetaTmpBucket, tmpDir), targetPath, pathJoin(drivePath, minioMetaBucket))
			}
			wait()
			return nil
		})
	})
}

// ListMultipartUploads - lists all the pending multipart
// uploads for a particular object in a bucket.
//
// Implements minimal S3 compatible ListMultipartUploads API. We do
// not support prefix based listing, this is a deliberate attempt
// towards simplification of multipart APIs.
// The resulting ListMultipartsInfo structure is unmarshalled directly as XML.
func (er erasureObjects) ListMultipartUploads(ctx context.Context, bucket, object, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartsInfo, err error) {
	auditObjectErasureSet(ctx, "ListMultipartUploads", object, &er)

	result.MaxUploads = maxUploads
	result.KeyMarker = keyMarker
	result.Prefix = object
	result.Delimiter = delimiter

	var uploadIDs []string
	var disk StorageAPI
	// disks := er.getOnlineLocalDisks()
	activeDisks, _, _ := er.GetActiveInfo(ctx, er.getDisks(), "ListMultipartUploads")
	if len(activeDisks) == 0 {
		// If no local, get non-healing disks.
		var ok bool
		if activeDisks, ok = er.getOnlineDisksWithHealing(false); !ok {
			activeDisks = er.getOnlineDisks()
		}
	}

	for _, disk = range activeDisks {
		if disk == nil {
			continue
		}
		if !disk.IsOnline() {
			continue
		}
		uploadIDs, err = disk.ListDir(ctx, bucket, minioMetaMultipartBucket, er.getMultipartSHADir(bucket, object), -1)
		if err != nil {
			if errors.Is(err, errDiskNotFound) {
				continue
			}
			if errors.Is(err, errFileNotFound) {
				return result, nil
			}
			return result, toObjectErr(err, bucket, object)
		}
		break
	}

	for i := range uploadIDs {
		uploadIDs[i] = strings.TrimSuffix(uploadIDs[i], SlashSeparator)
	}

	// S3 spec says uploadIDs should be sorted based on initiated time, we need
	// to read the metadata entry.
	var uploads []MultipartInfo

	populatedUploadIDs := set.NewStringSet()

	for _, uploadID := range uploadIDs {
		if populatedUploadIDs.Contains(uploadID) {
			continue
		}
		// If present, use time stored in ID.
		startTime := time.Now()
		if split := strings.Split(uploadID, "x"); len(split) == 2 {
			t, err := strconv.ParseInt(split[1], 10, 64)
			if err == nil {
				startTime = time.Unix(0, t)
			}
		}
		uploads = append(uploads, MultipartInfo{
			Bucket:    bucket,
			Object:    object,
			UploadID:  base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("%s.%s", globalDeploymentID(), uploadID))),
			Initiated: startTime,
		})
		populatedUploadIDs.Add(uploadID)
	}

	sort.Slice(uploads, func(i int, j int) bool {
		return uploads[i].Initiated.Before(uploads[j].Initiated)
	})

	uploadIndex := 0
	if uploadIDMarker != "" {
		for uploadIndex < len(uploads) {
			if uploads[uploadIndex].UploadID != uploadIDMarker {
				uploadIndex++
				continue
			}
			if uploads[uploadIndex].UploadID == uploadIDMarker {
				uploadIndex++
				break
			}
			uploadIndex++
		}
	}
	for uploadIndex < len(uploads) {
		result.Uploads = append(result.Uploads, uploads[uploadIndex])
		result.NextUploadIDMarker = uploads[uploadIndex].UploadID
		uploadIndex++
		if len(result.Uploads) == maxUploads {
			break
		}
	}

	result.IsTruncated = uploadIndex < len(uploads)

	if !result.IsTruncated {
		result.NextKeyMarker = ""
		result.NextUploadIDMarker = ""
	}

	return result, nil
}

// newMultipartUpload - wrapper for initializing a new multipart
// request; returns a unique upload id.
//
// Internally this function creates 'uploads.json' associated for the
// incoming object at
// '.minio.sys/multipart/bucket/object/uploads.json' on all the
// disks. `uploads.json` carries metadata regarding on-going multipart
// operation(s) on the object.
func (er erasureObjects) newMultipartUpload(ctx context.Context, bucket string, object string, opts ObjectOptions) (*NewMultipartUploadResult, error) {
	if opts.CheckPrecondFn != nil {
		if !opts.NoLock {
			ns := er.NewNSLock(bucket, object)
			lkctx, err := ns.GetLock(ctx, globalOperationTimeout)
			if err != nil {
				return nil, err
			}
			ctx = lkctx.Context()
			defer ns.Unlock(lkctx)
			opts.NoLock = true
		}

		obj, err := er.getObjectInfo(ctx, bucket, object, opts)
		if err == nil && opts.CheckPrecondFn(obj) {
			return nil, PreConditionFailed{}
		}
		if err != nil && !isErrVersionNotFound(err) && !isErrObjectNotFound(err) && !isErrReadQuorum(err) {
			return nil, err
		}
	}

	userDefined := cloneMSS(opts.UserDefined)
	if opts.PreserveETag != "" {
		userDefined["etag"] = opts.PreserveETag
	}
	onlineDisks := er.getDisks()
	logger.LogIf(ctx, "", fmt.Errorf("[YBS] onlineDisks 갯수: %v", len(onlineDisks)))
	for i, disk := range onlineDisks {
		if disk == nil {
			logger.LogIf(ctx, "", fmt.Errorf("[YBS] 디스크[%d]: nil", i))
			continue
		}

		isOnline := disk.IsOnline()
		isLocal := disk.IsLocal()
		diskInfo := disk.String()
		logger.LogIf(
			ctx,
			"erasure-multipart.newMultipartUpload",
			fmt.Errorf("[YBS] 디스크[%d]: %s, 온라인 상태: %v, 로컬 상태: %v", i, diskInfo, isOnline, isLocal),
		)
	}

	// Get parity and data drive count based on storage class metadata
	parityDrives := globalStorageClass.GetParityForSC(userDefined[xhttp.AmzStorageClass])
	logger.LogIf(ctx, "", fmt.Errorf("[YBS] parityDrives step1: %d\n", parityDrives))

	if parityDrives < 0 {
		parityDrives = er.defaultParityCount
	}
	logger.LogIf(
		ctx,
		"erasure-multipart.newMultipartUpload",
		fmt.Errorf("[YBS] parityDrives step2: %d, globalStorageClass.AvailabilityOptimized():%v", parityDrives, globalStorageClass.AvailabilityOptimized()),
	)
	if globalStorageClass.AvailabilityOptimized() {
		// If we have offline disks upgrade the number of erasure codes for this object.
		parityOrig := parityDrives

		var offlineDrives int
		for _, disk := range onlineDisks {
			if disk == nil || !disk.IsOnline() {
				parityDrives++
				offlineDrives++
				continue
			}
		}

		logger.LogIf(
			ctx,
			"erasure-multipart.newMultipartUpload",
			fmt.Errorf("[YBS] offlineDrives: %d, (len(onlineDisks)+1)/2: %d", offlineDrives, (len(onlineDisks)+1)/2),
		)
		if offlineDrives >= (len(onlineDisks)+1)/2 {
			// if offline drives are more than 50% of the drives
			// we have no quorum, we shouldn't proceed just
			// fail at that point.
			return nil, toObjectErr(errErasureWriteQuorum, bucket, object)
		}

		logger.LogIf(
			ctx,
			"erasure-multipart.newMultipartUpload",
			fmt.Errorf("[YBS] parityDrives >= len(onlineDisks)/2: %d >= %d", parityDrives, len(onlineDisks)/2),
		)
		if parityDrives >= len(onlineDisks)/2 {
			parityDrives = len(onlineDisks) / 2
		}
		logger.LogIf(
			ctx,
			"erasure-multipart.newMultipartUpload",
			fmt.Errorf("[YBS] parityDrives: %d, parityOrig: %d", parityDrives, parityOrig),
		)
		if parityOrig != parityDrives {
			userDefined[minIOErasureUpgraded] = strconv.Itoa(parityOrig) + "->" + strconv.Itoa(parityDrives)
		}
	}

	dataDrives := len(onlineDisks) - parityDrives
	logger.LogIf(
		ctx,
		"erasure-multipart.newMultipartUpload",
		fmt.Errorf("[YBS] dataDrives: %d, len(onlineDisks): %d, parityDrives: %d", dataDrives, len(onlineDisks), parityDrives),
	)
	// we now know the number of blocks this object needs for data and parity.
	// establish the writeQuorum using this data
	writeQuorum := dataDrives
	if dataDrives == parityDrives {
		writeQuorum++
	}

	// Initialize parts metadata
	partsMetadata := make([]FileInfo, len(onlineDisks))

	fi := newFileInfo(pathJoin(bucket, object), dataDrives, parityDrives)
	fi.VersionID = opts.VersionID
	if opts.Versioned && fi.VersionID == "" {
		fi.VersionID = mustGetUUID()
	}
	fi.DataDir = mustGetUUID()

	if ckSum := userDefined[ReplicationSsecChecksumHeader]; ckSum != "" {
		v, err := base64.StdEncoding.DecodeString(ckSum)
		if err == nil {
			fi.Checksum = v
		}
		delete(userDefined, ReplicationSsecChecksumHeader)
	}

	// Initialize erasure metadata.
	for index := range partsMetadata {
		partsMetadata[index] = fi
	}

	// Guess content-type from the extension if possible.
	if userDefined["content-type"] == "" {
		userDefined["content-type"] = mimedb.TypeByExtension(path.Ext(object))
	}

	// if storageClass is standard no need to save it as part of metadata.
	if userDefined[xhttp.AmzStorageClass] == storageclass.STANDARD {
		delete(userDefined, xhttp.AmzStorageClass)
	}

	if opts.WantChecksum != nil && opts.WantChecksum.Type.IsSet() {
		userDefined[hash.MinIOMultipartChecksum] = opts.WantChecksum.Type.String()
	}

	modTime := opts.MTime
	if opts.MTime.IsZero() {
		modTime = UTCNow()
	}

	onlineDisks, partsMetadata = shuffleDisksAndPartsMetadata(onlineDisks, partsMetadata, fi)
	logger.LogIf(ctx, "", fmt.Errorf("[YBS] onlineDisks 개수: %d", len(onlineDisks)))
	for i, disk := range onlineDisks {
		if disk == nil {
			logger.LogIf(ctx, "", fmt.Errorf("[YBS] onlineDisks[%d]: nil", i))
		} else {
			logger.LogIf(ctx, "", fmt.Errorf("[YBS] onlineDisks[%d]: %s, 온라인 상태: %v", i, disk.String(), disk.IsOnline()))
		}
	}

	logger.LogIf(ctx, "", fmt.Errorf("[YBS] partsMetadata 개수: %d", len(partsMetadata)))
	for i, part := range partsMetadata {
		logger.LogIf(ctx, "", fmt.Errorf("[YBS] partsMetadata[%d]: DataBlocks=%d, ParityBlocks=%d, Distribution=%v",
			i, part.Erasure.DataBlocks, part.Erasure.ParityBlocks, part.Erasure.Distribution))
	}

	// Fill all the necessary metadata.
	// Update `xl.meta` content on each disks.
	for index := range partsMetadata {
		partsMetadata[index].Fresh = true
		partsMetadata[index].ModTime = modTime
		partsMetadata[index].Metadata = userDefined
	}
	uploadUUID := fmt.Sprintf("%sx%d", mustGetUUID(), modTime.UnixNano())
	uploadID := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("%s.%s", globalDeploymentID(), uploadUUID)))
	uploadIDPath := er.getUploadIDDir(bucket, object, uploadUUID)

	// Write updated `xl.meta` to all disks.
	if _, err := writeAllMetadata(ctx, onlineDisks, bucket, minioMetaMultipartBucket, uploadIDPath, partsMetadata, writeQuorum); err != nil {
		return nil, toObjectErr(err, bucket, object)
	}

	return &NewMultipartUploadResult{
		UploadID:     uploadID,
		ChecksumAlgo: userDefined[hash.MinIOMultipartChecksum],
	}, nil
}

// NewMultipartUpload - initialize a new multipart upload, returns a
// unique id. The unique id returned here is of UUID form, for each
// subsequent request each UUID is unique.
//
// Implements S3 compatible initiate multipart API.
func (er erasureObjects) NewMultipartUpload(ctx context.Context, bucket, object string, opts ObjectOptions) (*NewMultipartUploadResult, error) {
	if !opts.NoAuditLog {
		auditObjectErasureSet(ctx, "NewMultipartUpload", object, &er)
	}
	if strings.HasPrefix(bucket, ".") || strings.HasPrefix(object, ".") {
		logger.LogIf(ctx, "", fmt.Errorf("[YBS] NewMultipartUpload->newMultipartUpload bucket : %s AND object : %s", bucket, object))
		return er.newMultipartUpload(ctx, bucket, object, opts)
	} else {
		logger.LogIf(ctx, "", fmt.Errorf("[YBS] NewMultipartUpload->newMultipartUploadIDC bucket : %s AND object : %s", bucket, object))
		return er.newMultipartUploadIDC(ctx, bucket, object, opts)
	}
}

func (er erasureObjects) newMultipartUploadIDC(ctx context.Context, bucket string, object string, opts ObjectOptions) (*NewMultipartUploadResult, error) {
	modTime := opts.MTime
	if opts.MTime.IsZero() {
		modTime = UTCNow()
	}
	uploadUUID := fmt.Sprintf("%sx%d", mustGetUUID(), modTime.UnixNano())
	uploadID := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("%s.%s", globalDeploymentID(), uploadUUID)))
	multipartLatency.RecordMultipartStart(ctx, bucket, object, uploadID, time.Now())

	if opts.CheckPrecondFn != nil {
		if !opts.NoLock {
			ns := er.NewNSLock(bucket, object)
			lkctx, err := ns.GetLock(ctx, globalOperationTimeout)
			if err != nil {
				return nil, err
			}
			ctx = lkctx.Context()
			defer ns.Unlock(lkctx)
			opts.NoLock = true
		}

		obj, err := er.getObjectInfo(ctx, bucket, object, opts)
		if err == nil && opts.CheckPrecondFn(obj) {
			return nil, PreConditionFailed{}
		}
		if err != nil && !isErrVersionNotFound(err) && !isErrObjectNotFound(err) && !isErrReadQuorum(err) {
			return nil, err
		}
	}

	userDefined := cloneMSS(opts.UserDefined)
	if opts.PreserveETag != "" {
		userDefined["etag"] = opts.PreserveETag
	}

	activeDisks, _, activeIDCCount := er.GetActiveInfo(ctx, er.getDisks(), "newMultipartUploadIDC")
	logger.LogIf(ctx, "", fmt.Errorf("[YBS] newMultipartUploadIDC activeDisks 갯수: %v", len(activeDisks)))

	activeDisks, dataDrives, parityDrives, returnFlag := er.DecideErasureCodingParameter(ctx, activeDisks, activeIDCCount)
	if returnFlag {
		return nil, toObjectErr(errErasureWriteQuorum, bucket, object)
	}

	// we now know the number of blocks this object needs for data and parity.
	// establish the writeQuorum using this data
	writeQuorum := dataDrives
	if dataDrives == parityDrives {
		writeQuorum++
	}

	// Initialize parts metadata
	partsMetadata := make([]FileInfo, len(activeDisks))

	fi := newFileInfo(pathJoin(bucket, object), dataDrives, parityDrives)
	fi.Metadata = userDefined
	userDefined["ec_data_blocks"] = strconv.Itoa(dataDrives)
	userDefined["ec_parity_blocks"] = strconv.Itoa(parityDrives)
	userDefined["ec_distribution"] = fmt.Sprintf("%v", fi.Erasure.Distribution)

	fi.VersionID = opts.VersionID
	if opts.Versioned && fi.VersionID == "" {
		fi.VersionID = mustGetUUID()
	}
	fi.DataDir = mustGetUUID()

	if ckSum := userDefined[ReplicationSsecChecksumHeader]; ckSum != "" {
		v, err := base64.StdEncoding.DecodeString(ckSum)
		if err == nil {
			fi.Checksum = v
		}
		delete(userDefined, ReplicationSsecChecksumHeader)
	}

	// Initialize erasure metadata.
	for index := range partsMetadata {
		partsMetadata[index] = fi
	}

	// Guess content-type from the extension if possible.
	if userDefined["content-type"] == "" {
		userDefined["content-type"] = mimedb.TypeByExtension(path.Ext(object))
	}

	// if storageClass is standard no need to save it as part of metadata.
	if userDefined[xhttp.AmzStorageClass] == storageclass.STANDARD {
		delete(userDefined, xhttp.AmzStorageClass)
	}

	if opts.WantChecksum != nil && opts.WantChecksum.Type.IsSet() {
		userDefined[hash.MinIOMultipartChecksum] = opts.WantChecksum.Type.String()
	}

	// modTime := opts.MTime
	// if opts.MTime.IsZero() {
	// 	modTime = UTCNow()
	// }

	activeDisks, partsMetadata = shuffleDisksAndPartsMetadata(activeDisks, partsMetadata, fi)

	for i, part := range partsMetadata {
		logger.LogIf(ctx, "", fmt.Errorf("[YBS] newMultipartUploadIDC partsMetadata[%d]: DataBlocks=%d, ParityBlocks=%d, Distribution=%v",
			i, part.Erasure.DataBlocks, part.Erasure.ParityBlocks, part.Erasure.Distribution))
	}

	// Fill all the necessary metadata.
	// Update `xl.meta` content on each disks.
	for index := range partsMetadata {
		partsMetadata[index].Fresh = true
		partsMetadata[index].ModTime = modTime
		partsMetadata[index].Metadata = userDefined
	}
	// uploadUUID := fmt.Sprintf("%sx%d", mustGetUUID(), modTime.UnixNano())
	// uploadID := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("%s.%s", globalDeploymentID(), uploadUUID)))
	uploadIDPath := er.getUploadIDDir(bucket, object, uploadUUID)

	// Write updated `xl.meta` to all disks.
	if _, err := writeAllMetadata(ctx, activeDisks, bucket, minioMetaMultipartBucket, uploadIDPath, partsMetadata, writeQuorum); err != nil {
		return nil, toObjectErr(err, bucket, object)
	}

	return &NewMultipartUploadResult{
		UploadID:     uploadID,
		ChecksumAlgo: userDefined[hash.MinIOMultipartChecksum],
	}, nil
}

// renamePart - renames multipart part to its relevant location under uploadID.
func (er erasureObjects) renamePart(ctx context.Context, disks []StorageAPI, srcBucket, srcEntry, dstBucket, dstEntry string, optsMeta []byte, writeQuorum int) ([]StorageAPI, error) {
	paths := []string{
		dstEntry,
		dstEntry + ".meta",
	}

	// cleanup existing paths first across all drives.
	er.cleanupMultipartPath(ctx, disks, paths...)

	g := errgroup.WithNErrs(len(disks))

	// Rename file on all underlying storage disks.
	for index := range disks {
		index := index
		g.Go(func() error {
			if disks[index] == nil {
				return errDiskNotFound
			}
			return disks[index].RenamePart(ctx, srcBucket, srcEntry, dstBucket, dstEntry, optsMeta)
		}, index)
	}

	// Wait for all renames to finish.
	errs := g.Wait()

	err := reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, writeQuorum)
	if err != nil {
		er.cleanupMultipartPath(ctx, disks, paths...)
	}

	// We can safely allow RenameFile errors up to len(er.getDisks()) - writeQuorum
	// otherwise return failure. Cleanup successful renames.
	return evalDisks(disks, errs), err
}

// PutObjectPart - reads incoming stream and internally erasure codes
// them. This call is similar to single put operation but it is part
// of the multipart transaction.
//
// Implements S3 compatible Upload Part API.
func (er erasureObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, r *PutObjReader, opts ObjectOptions) (pi PartInfo, err error) {
	if !opts.NoAuditLog {
		auditObjectErasureSet(ctx, "PutObjectPart", object, &er)
	}
	if strings.HasPrefix(bucket, ".") || strings.HasPrefix(object, ".") {
		logger.LogIf(ctx, "", fmt.Errorf("[YBS] PutObjectPart->putObjectPart bucket : %s AND object : %s", bucket, object))
		return er.putObjectPart(ctx, bucket, object, uploadID, partID, r, opts, pi, err)
	} else {
		logger.LogIf(ctx, "", fmt.Errorf("[YBS] PutObjectPart->putObjectPartIDC bucket : %s AND object : %s", bucket, object))
		return er.putObjectPartIDC(ctx, bucket, object, uploadID, partID, r, opts, pi, err)
	}
}

func (er erasureObjects) putObjectPart(ctx context.Context, bucket string, object string, uploadID string, partID int, r *PutObjReader, opts ObjectOptions, pi PartInfo, err error) (PartInfo, error) {
	data := r.Reader
	// Validate input data size and it can never be less than zero.
	if data.Size() < -1 {
		bugLogIf(ctx, errInvalidArgument, logger.ErrorKind)
		return pi, toObjectErr(errInvalidArgument)
	}

	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)
	// Validates if upload ID exists.
	fi, _, err := er.checkUploadIDExistsOriginal(ctx, bucket, object, uploadID, true)
	if err != nil {
		if errors.Is(err, errVolumeNotFound) {
			return pi, toObjectErr(err, bucket)
		}
		return pi, toObjectErr(err, bucket, object, uploadID)
	}

	onlineDisks := er.getDisks()
	logger.LogIf(ctx, "", fmt.Errorf("[YBS] onlineDisks 개수: %d", len(onlineDisks)))
	for i, disk := range onlineDisks {
		if disk == nil {
			logger.LogIf(ctx, "", fmt.Errorf("[YBS] onlineDisks[%d]: nil", i))
		}
	}
	writeQuorum := fi.WriteQuorum(er.defaultWQuorum())
	if cs := fi.Metadata[hash.MinIOMultipartChecksum]; cs != "" {
		if r.ContentCRCType().String() != cs {
			return pi, InvalidArgument{
				Bucket: bucket,
				Object: fi.Name,
				Err:    fmt.Errorf("checksum missing, want %q, got %q", cs, r.ContentCRCType().String()),
			}
		}
	}
	logger.LogIf(ctx, "", fmt.Errorf("[YBS] fi.Erasure.Distribution: %v", fi.Erasure.Distribution))
	onlineDisks = shuffleDisks(onlineDisks, fi.Erasure.Distribution)

	// Need a unique name for the part being written in minioMetaBucket to
	// accommodate concurrent PutObjectPart requests

	partSuffix := fmt.Sprintf("part.%d", partID)
	// Random UUID and timestamp for temporary part file.
	tmpPart := fmt.Sprintf("%sx%d", mustGetUUID(), time.Now().UnixNano())
	tmpPartPath := pathJoin(tmpPart, partSuffix)

	// Delete the temporary object part. If PutObjectPart succeeds there would be nothing to delete.
	defer func() {
		if countOnlineDisks(onlineDisks) != len(onlineDisks) {
			er.deleteAll(context.Background(), minioMetaTmpBucket, tmpPart)
		}
	}()

	logger.LogIf(ctx, "", fmt.Errorf("[YBS] fi.Erasure.DataBlocks: %d, fi.Erasure.ParityBlocks: %d, fi.Erasure.BlockSize: %d\n", fi.Erasure.DataBlocks, fi.Erasure.ParityBlocks, fi.Erasure.BlockSize))
	erasure, err := NewErasure(ctx, fi.Erasure.DataBlocks, fi.Erasure.ParityBlocks, fi.Erasure.BlockSize)
	if err != nil {
		return pi, toObjectErr(err, bucket, object)
	}

	// Fetch buffer for I/O, returns from the pool if not allocates a new one and returns.
	var buffer []byte
	switch size := data.Size(); {
	case size == 0:
		buffer = make([]byte, 1) // Allocate at least a byte to reach EOF
	case size == -1:
		if size := data.ActualSize(); size > 0 && size < fi.Erasure.BlockSize {
			// Account for padding and forced compression overhead and encryption.
			buffer = make([]byte, data.ActualSize()+256+32+32, data.ActualSize()*2+512)
		} else {
			buffer = globalBytePoolCap.Load().Get()
			defer globalBytePoolCap.Load().Put(buffer)
		}
	case size >= fi.Erasure.BlockSize:
		buffer = globalBytePoolCap.Load().Get()
		defer globalBytePoolCap.Load().Put(buffer)
	case size < fi.Erasure.BlockSize:
		// No need to allocate fully fi.Erasure.BlockSize buffer if the incoming data is smaller.
		buffer = make([]byte, size, 2*size+int64(fi.Erasure.ParityBlocks+fi.Erasure.DataBlocks-1))
	}

	if len(buffer) > int(fi.Erasure.BlockSize) {
		buffer = buffer[:fi.Erasure.BlockSize]
	}
	writers := make([]io.Writer, len(onlineDisks))
	for i, disk := range onlineDisks {
		if disk == nil {
			continue
		}
		writers[i] = newBitrotWriter(disk, bucket, minioMetaTmpBucket, tmpPartPath, erasure.ShardFileSize(data.Size()), DefaultBitrotAlgorithm, erasure.ShardSize())
	}

	toEncode := io.Reader(data)
	if data.Size() > bigFileThreshold {
		// Add input readahead.
		// We use 2 buffers, so we always have a full buffer of input.
		pool := globalBytePoolCap.Load()
		bufA := pool.Get()
		bufB := pool.Get()
		defer pool.Put(bufA)
		defer pool.Put(bufB)
		ra, err := readahead.NewReaderBuffer(data, [][]byte{bufA[:fi.Erasure.BlockSize], bufB[:fi.Erasure.BlockSize]})
		if err == nil {
			toEncode = ra
			defer ra.Close()
		}
	}

	n, err := erasure.Encode(ctx, toEncode, writers, buffer, writeQuorum)
	closeBitrotWriters(writers)
	if err != nil {
		return pi, toObjectErr(err, bucket, object)
	}

	// Should return IncompleteBody{} error when reader has fewer bytes
	// than specified in request header.
	if n < data.Size() {
		return pi, IncompleteBody{Bucket: bucket, Object: object}
	}

	for i := range writers {
		if writers[i] == nil {
			onlineDisks[i] = nil
		}
	}

	// Rename temporary part file to its final location.
	partPath := pathJoin(uploadIDPath, fi.DataDir, partSuffix)

	md5hex := r.MD5CurrentHexString()
	if opts.PreserveETag != "" {
		md5hex = opts.PreserveETag
	}

	var index []byte
	if opts.IndexCB != nil {
		index = opts.IndexCB()
	}

	actualSize := data.ActualSize()
	if actualSize < 0 {
		_, encrypted := crypto.IsEncrypted(fi.Metadata)
		compressed := fi.IsCompressed()
		switch {
		case compressed:
			// ... nothing changes for compressed stream.
			// if actualSize is -1 we have no known way to
			// determine what is the actualSize.
		case encrypted:
			decSize, err := sio.DecryptedSize(uint64(n))
			if err == nil {
				actualSize = int64(decSize)
			}
		default:
			actualSize = n
		}
	}

	partInfo := ObjectPartInfo{
		Number:     partID,
		ETag:       md5hex,
		Size:       n,
		ActualSize: actualSize,
		ModTime:    UTCNow(),
		Index:      index,
		Checksums:  r.ContentCRC(),
	}

	partFI, err := partInfo.MarshalMsg(nil)
	if err != nil {
		return pi, toObjectErr(err, minioMetaMultipartBucket, partPath)
	}

	// Serialize concurrent part uploads.
	partIDLock := er.NewNSLock(bucket, pathJoin(object, uploadID, strconv.Itoa(partID)))
	plkctx, err := partIDLock.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return PartInfo{}, err
	}

	ctx = plkctx.Context()
	defer partIDLock.Unlock(plkctx)

	onlineDisks, err = er.renamePart(ctx, onlineDisks, minioMetaTmpBucket, tmpPartPath, minioMetaMultipartBucket, partPath, partFI, writeQuorum)
	if err != nil {
		if errors.Is(err, errFileNotFound) {
			// An in-quorum errFileNotFound means that client stream
			// prematurely closed and we do not find any xl.meta or
			// part.1's - in such a scenario we must return as if client
			// disconnected. This means that erasure.Encode() CreateFile()
			// did not do anything.
			return pi, IncompleteBody{Bucket: bucket, Object: object}
		}

		return pi, toObjectErr(err, minioMetaMultipartBucket, partPath)
	}

	// Return success.
	return PartInfo{
		PartNumber:     partInfo.Number,
		ETag:           partInfo.ETag,
		LastModified:   partInfo.ModTime,
		Size:           partInfo.Size,
		ActualSize:     partInfo.ActualSize,
		ChecksumCRC32:  partInfo.Checksums["CRC32"],
		ChecksumCRC32C: partInfo.Checksums["CRC32C"],
		ChecksumSHA1:   partInfo.Checksums["SHA1"],
		ChecksumSHA256: partInfo.Checksums["SHA256"],
	}, nil
}

func (er erasureObjects) putObjectPartIDC(ctx context.Context, bucket string, object string, uploadID string, partID int, r *PutObjReader, opts ObjectOptions, pi PartInfo, err error) (PartInfo, error) {
	// [YBS_DEBUG] checkUploadIDExists CALLED 로그는 putObjectPartIDC의 스코프에 write 변수가 없으므로 제거합니다.
	// checkUploadIDExists 함수 자체의 시작점에 유사한 로그가 이미 존재합니다.
	defer multipartLatency.MesurePutObjectPartElapsed(ctx, bucket, object, uploadID, partID)()
	data := r.Reader
	// Validate input data size and it can never be less than zero.
	if data.Size() < -1 {
		bugLogIf(ctx, errInvalidArgument, logger.ErrorKind)
		return pi, toObjectErr(errInvalidArgument)
	}

	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)
	// Validates if upload ID exists.
	fi, _, activeDisks, err := er.checkUploadIDExists(ctx, bucket, object, uploadID, true)
	if err != nil {
		logger.LogIf(ctx, "", fmt.Errorf("[YBS] putObjectPartIDC err: %v", err))
		if errors.Is(err, errVolumeNotFound) {
			return pi, toObjectErr(err, bucket)
		}
		return pi, toObjectErr(err, bucket, object, uploadID)
	}

	initialDataBlocksStr := fi.Metadata["ec_data_blocks"]
	initialParityBlocksStr := fi.Metadata["ec_parity_blocks"]
	if initialDataBlocksStr != "" && initialParityBlocksStr != "" {
		logger.LogIf(ctx, "", fmt.Errorf("[YBS] initialDataBlocksStr: %s, initialParityBlocksStr: %s", initialDataBlocksStr, initialParityBlocksStr))
		initialDataBlocks, _ := strconv.Atoi(initialDataBlocksStr)
		initialParityBlocks, _ := strconv.Atoi(initialParityBlocksStr)

		// 현재 활성 EC 파라미터와 비교
		currentDataBlocks := fi.Erasure.DataBlocks
		currentParityBlocks := fi.Erasure.ParityBlocks

		logger.LogIf(ctx, "", fmt.Errorf("[YBS] currentDataBlocks: %d, currentParityBlocks: %d", currentDataBlocks, currentParityBlocks))

		// EC 설정이 변경됐는지 확인
		if initialDataBlocks != currentDataBlocks || initialParityBlocks != currentParityBlocks {
			logger.LogIf(ctx, "", fmt.Errorf("EC configuration changed during multipart upload: initial[%d+%d] current[%d+%d]",
				initialDataBlocks, initialParityBlocks, currentDataBlocks, currentParityBlocks))

			return pi, toObjectErr(errors.New("IDC topology changed during upload - please abort and retry the multipart upload"), bucket, object, uploadID)
		}
	}

	// onlineDisks := er.getDisks()
	writeQuorum := fi.WriteQuorum(er.defaultWQuorum())
	if cs := fi.Metadata[hash.MinIOMultipartChecksum]; cs != "" {
		if r.ContentCRCType().String() != cs {
			return pi, InvalidArgument{
				Bucket: bucket,
				Object: fi.Name,
				Err:    fmt.Errorf("checksum missing, want %q, got %q", cs, r.ContentCRCType().String()),
			}
		}
	}
	logger.LogIf(ctx, "", fmt.Errorf("[YBS] putObjectPartIDC fi.Erasure.Distribution: %v", fi.Erasure.Distribution))
	activeDisks = shuffleDisks(activeDisks, fi.Erasure.Distribution)
	logger.LogIf(ctx, "", fmt.Errorf("[YBS] putObjectPartIDC activeDisks: %v", activeDisks))
	// Need a unique name for the part being written in minioMetaBucket to
	// accommodate concurrent PutObjectPart requests

	partSuffix := fmt.Sprintf("part.%d", partID)
	// Random UUID and timestamp for temporary part file.
	tmpPart := fmt.Sprintf("%sx%d", mustGetUUID(), time.Now().UnixNano())
	tmpPartPath := pathJoin(tmpPart, partSuffix)

	// Delete the temporary object part. If PutObjectPart succeeds there would be nothing to delete.
	defer func() {
		if countOnlineDisks(activeDisks) != len(activeDisks) {
			er.deleteAll(context.Background(), minioMetaTmpBucket, tmpPart)
		}
	}()

	logger.LogIf(ctx, "", fmt.Errorf("[YBS] putObjectPartIDC fi.Erasure.DataBlocks: %d, fi.Erasure.ParityBlocks: %d, fi.Erasure.BlockSize: %d", fi.Erasure.DataBlocks, fi.Erasure.ParityBlocks, fi.Erasure.BlockSize))
	erasure, err := NewErasure(ctx, fi.Erasure.DataBlocks, fi.Erasure.ParityBlocks, fi.Erasure.BlockSize)
	if err != nil {
		return pi, toObjectErr(err, bucket, object)
	}

	// Fetch buffer for I/O, returns from the pool if not allocates a new one and returns.
	var buffer []byte
	switch size := data.Size(); {
	case size == 0:
		buffer = make([]byte, 1) // Allocate at least a byte to reach EOF
	case size == -1:
		if size := data.ActualSize(); size > 0 && size < fi.Erasure.BlockSize {
			// Account for padding and forced compression overhead and encryption.
			buffer = make([]byte, data.ActualSize()+256+32+32, data.ActualSize()*2+512)
		} else {
			buffer = globalBytePoolCap.Load().Get()
			defer globalBytePoolCap.Load().Put(buffer)
		}
	case size >= fi.Erasure.BlockSize:
		buffer = globalBytePoolCap.Load().Get()
		defer globalBytePoolCap.Load().Put(buffer)
	case size < fi.Erasure.BlockSize:
		// No need to allocate fully fi.Erasure.BlockSize buffer if the incoming data is smaller.
		buffer = make([]byte, size, 2*size+int64(fi.Erasure.ParityBlocks+fi.Erasure.DataBlocks-1))
	}

	if len(buffer) > int(fi.Erasure.BlockSize) {
		buffer = buffer[:fi.Erasure.BlockSize]
	}
	writers := make([]io.Writer, len(activeDisks))
	for i, disk := range activeDisks {
		if disk == nil {
			continue
		}
		writers[i] = newBitrotWriter(disk, bucket, minioMetaTmpBucket, tmpPartPath, erasure.ShardFileSize(data.Size()), DefaultBitrotAlgorithm, erasure.ShardSize())
	}

	toEncode := io.Reader(data)
	if data.Size() > bigFileThreshold {
		// Add input readahead.
		// We use 2 buffers, so we always have a full buffer of input.
		pool := globalBytePoolCap.Load()
		bufA := pool.Get()
		bufB := pool.Get()
		defer pool.Put(bufA)
		defer pool.Put(bufB)
		ra, err := readahead.NewReaderBuffer(data, [][]byte{bufA[:fi.Erasure.BlockSize], bufB[:fi.Erasure.BlockSize]})
		if err == nil {
			toEncode = ra
			defer ra.Close()
		}
	}

	n, err := erasure.Encode(ctx, toEncode, writers, buffer, writeQuorum)
	closeBitrotWriters(writers)
	if err != nil {
		return pi, toObjectErr(err, bucket, object)
	}

	// Should return IncompleteBody{} error when reader has fewer bytes
	// than specified in request header.
	if n < data.Size() {
		return pi, IncompleteBody{Bucket: bucket, Object: object}
	}

	for i := range writers {
		if writers[i] == nil {
			activeDisks[i] = nil
		}
	}

	// Rename temporary part file to its final location.
	partPath := pathJoin(uploadIDPath, fi.DataDir, partSuffix)

	md5hex := r.MD5CurrentHexString()
	if opts.PreserveETag != "" {
		md5hex = opts.PreserveETag
	}

	var index []byte
	if opts.IndexCB != nil {
		index = opts.IndexCB()
	}

	actualSize := data.ActualSize()
	if actualSize < 0 {
		_, encrypted := crypto.IsEncrypted(fi.Metadata)
		compressed := fi.IsCompressed()
		switch {
		case compressed:
			// ... nothing changes for compressed stream.
			// if actualSize is -1 we have no known way to
			// determine what is the actualSize.
		case encrypted:
			decSize, err := sio.DecryptedSize(uint64(n))
			if err == nil {
				actualSize = int64(decSize)
			}
		default:
			actualSize = n
		}
	}

	partInfo := ObjectPartInfo{
		Number:     partID,
		ETag:       md5hex,
		Size:       n,
		ActualSize: actualSize,
		ModTime:    UTCNow(),
		Index:      index,
		Checksums:  r.ContentCRC(),
	}

	partFI, err := partInfo.MarshalMsg(nil)
	if err != nil {
		return pi, toObjectErr(err, minioMetaMultipartBucket, partPath)
	}

	// Serialize concurrent part uploads.
	partIDLock := er.NewNSLock(bucket, pathJoin(object, uploadID, strconv.Itoa(partID)))
	plkctx, err := partIDLock.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return PartInfo{}, err
	}

	ctx = plkctx.Context()
	defer partIDLock.Unlock(plkctx)

	activeDisks, err = er.renamePart(ctx, activeDisks, minioMetaTmpBucket, tmpPartPath, minioMetaMultipartBucket, partPath, partFI, writeQuorum)
	if err != nil {
		if errors.Is(err, errFileNotFound) {
			// An in-quorum errFileNotFound means that client stream
			// prematurely closed and we do not find any xl.meta or
			// part.1's - in such a scenario we must return as if client
			// disconnected. This means that erasure.Encode() CreateFile()
			// did not do anything.
			return pi, IncompleteBody{Bucket: bucket, Object: object}
		}

		return pi, toObjectErr(err, minioMetaMultipartBucket, partPath)
	}

	// Return success.
	return PartInfo{
		PartNumber:     partInfo.Number,
		ETag:           partInfo.ETag,
		LastModified:   partInfo.ModTime,
		Size:           partInfo.Size,
		ActualSize:     partInfo.ActualSize,
		ChecksumCRC32:  partInfo.Checksums["CRC32"],
		ChecksumCRC32C: partInfo.Checksums["CRC32C"],
		ChecksumSHA1:   partInfo.Checksums["SHA1"],
		ChecksumSHA256: partInfo.Checksums["SHA256"],
	}, nil
}

// GetMultipartInfo returns multipart metadata uploaded during newMultipartUpload, used
// by callers to verify object states
// - encrypted
// - compressed
// Does not contain currently uploaded parts by design.
func (er erasureObjects) GetMultipartInfo(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) (MultipartInfo, error) {
	if !opts.NoAuditLog {
		auditObjectErasureSet(ctx, "GetMultipartInfo", object, &er)
	}

	result := MultipartInfo{
		Bucket:   bucket,
		Object:   object,
		UploadID: uploadID,
	}

	fi, _, _, err := er.checkUploadIDExists(ctx, bucket, object, uploadID, false)
	if err != nil {
		if errors.Is(err, errVolumeNotFound) {
			return result, toObjectErr(err, bucket)
		}
		return result, toObjectErr(err, bucket, object, uploadID)
	}

	result.UserDefined = cloneMSS(fi.Metadata)
	return result, nil
}

func (er erasureObjects) listParts(ctx context.Context, onlineDisks []StorageAPI, partPath string, readQuorum int) ([]int, error) {
	g := errgroup.WithNErrs(len(onlineDisks))

	objectParts := make([][]string, len(onlineDisks))
	// List uploaded parts from drives.
	for index := range onlineDisks {
		index := index
		g.Go(func() (err error) {
			if onlineDisks[index] == nil {
				return errDiskNotFound
			}
			objectParts[index], err = onlineDisks[index].ListDir(ctx, minioMetaMultipartBucket, minioMetaMultipartBucket, partPath, -1)
			return err
		}, index)
	}

	if err := reduceReadQuorumErrs(ctx, g.Wait(), objectOpIgnoredErrs, readQuorum); err != nil {
		return nil, err
	}

	partQuorumMap := make(map[int]int)
	for _, driveParts := range objectParts {
		partsWithMetaCount := make(map[int]int, len(driveParts))
		// part files can be either part.N or part.N.meta
		for _, partPath := range driveParts {
			var partNum int
			if _, err := fmt.Sscanf(partPath, "part.%d", &partNum); err == nil {
				partsWithMetaCount[partNum]++
				continue
			}
			if _, err := fmt.Sscanf(partPath, "part.%d.meta", &partNum); err == nil {
				partsWithMetaCount[partNum]++
			}
		}
		// Include only part.N.meta files with corresponding part.N
		for partNum, cnt := range partsWithMetaCount {
			if cnt < 2 {
				continue
			}
			partQuorumMap[partNum]++
		}
	}

	var partNums []int
	for partNum, count := range partQuorumMap {
		if count < readQuorum {
			continue
		}
		partNums = append(partNums, partNum)
	}

	sort.Ints(partNums)
	return partNums, nil
}

// ListObjectParts - lists all previously uploaded parts for a given
// object and uploadID.  Takes additional input of part-number-marker
// to indicate where the listing should begin from.
//
// Implements S3 compatible ListObjectParts API. The resulting
// ListPartsInfo structure is marshaled directly into XML and
// replied back to the client.
func (er erasureObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int, opts ObjectOptions) (result ListPartsInfo, err error) {
	if !opts.NoAuditLog {
		auditObjectErasureSet(ctx, "ListObjectParts", object, &er)
	}

	fi, _, _, err := er.checkUploadIDExists(ctx, bucket, object, uploadID, false)
	if err != nil {
		return result, toObjectErr(err, bucket, object, uploadID)
	}

	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)
	if partNumberMarker < 0 {
		partNumberMarker = 0
	}

	// Limit output to maxPartsList.
	if maxParts > maxPartsList {
		maxParts = maxPartsList
	}

	// Populate the result stub.
	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	result.MaxParts = maxParts
	result.PartNumberMarker = partNumberMarker
	result.UserDefined = cloneMSS(fi.Metadata)
	result.ChecksumAlgorithm = fi.Metadata[hash.MinIOMultipartChecksum]

	if maxParts == 0 {
		return result, nil
	}

	// onlineDisks := er.getDisks()
	activeDisks, _, _ := er.GetActiveInfo(ctx, er.getDisks(), "listObjectPartsIDC")
	readQuorum := fi.ReadQuorum(er.defaultRQuorum())
	// Read Part info for all parts
	partPath := pathJoin(uploadIDPath, fi.DataDir) + SlashSeparator

	// List parts in quorum
	partNums, err := er.listParts(ctx, activeDisks, partPath, readQuorum)
	if err != nil {
		// This means that fi.DataDir, is not yet populated so we
		// return an empty response.
		if errors.Is(err, errFileNotFound) {
			return result, nil
		}
		return result, toObjectErr(err, bucket, object, uploadID)
	}

	if len(partNums) == 0 {
		return result, nil
	}

	start := objectPartIndexNums(partNums, partNumberMarker)
	if start != -1 {
		partNums = partNums[start+1:]
	}

	result.Parts = make([]PartInfo, 0, len(partNums))
	partMetaPaths := make([]string, len(partNums))
	for i, part := range partNums {
		partMetaPaths[i] = pathJoin(partPath, fmt.Sprintf("part.%d.meta", part))
	}

	// Read parts in quorum
	objParts, err := readParts(ctx, activeDisks, minioMetaMultipartBucket, partMetaPaths,
		partNums, readQuorum)
	if err != nil {
		return result, toObjectErr(err, bucket, object, uploadID)
	}

	count := maxParts
	for _, objPart := range objParts {
		result.Parts = append(result.Parts, PartInfo{
			PartNumber:     objPart.Number,
			LastModified:   objPart.ModTime,
			ETag:           objPart.ETag,
			Size:           objPart.Size,
			ActualSize:     objPart.ActualSize,
			ChecksumCRC32:  objPart.Checksums["CRC32"],
			ChecksumCRC32C: objPart.Checksums["CRC32C"],
			ChecksumSHA1:   objPart.Checksums["SHA1"],
			ChecksumSHA256: objPart.Checksums["SHA256"],
		})
		count--
		if count == 0 {
			break
		}
	}

	if len(objParts) > len(result.Parts) {
		result.IsTruncated = true
		// Make sure to fill next part number marker if IsTruncated is true for subsequent listing.
		result.NextPartNumberMarker = result.Parts[len(result.Parts)-1].PartNumber
	}

	return result, nil
}

func readParts(ctx context.Context, disks []StorageAPI, bucket string, partMetaPaths []string, partNumbers []int, readQuorum int) ([]ObjectPartInfo, error) {
	g := errgroup.WithNErrs(len(disks))

	objectPartInfos := make([][]*ObjectPartInfo, len(disks))
	// Rename file on all underlying storage disks.
	for index := range disks {
		index := index
		g.Go(func() (err error) {
			if disks[index] == nil {
				return errDiskNotFound
			}
			objectPartInfos[index], err = disks[index].ReadParts(ctx, bucket, partMetaPaths...)
			return err
		}, index)
	}

	if err := reduceReadQuorumErrs(ctx, g.Wait(), objectOpIgnoredErrs, readQuorum); err != nil {
		return nil, err
	}

	partInfosInQuorum := make([]ObjectPartInfo, len(partMetaPaths))
	for pidx := range partMetaPaths {
		// partMetaQuorumMap uses
		//  - path/to/part.N as key to collate errors from failed drives.
		//  - part ETag to collate part metadata
		partMetaQuorumMap := make(map[string]int, len(partNumbers))
		var pinfos []*ObjectPartInfo
		for idx := range disks {
			if len(objectPartInfos[idx]) != len(partMetaPaths) {
				partMetaQuorumMap[partMetaPaths[pidx]]++
				continue
			}

			pinfo := objectPartInfos[idx][pidx]
			if pinfo != nil && pinfo.ETag != "" {
				pinfos = append(pinfos, pinfo)
				partMetaQuorumMap[pinfo.ETag]++
				continue
			}
			partMetaQuorumMap[partMetaPaths[pidx]]++
		}

		var maxQuorum int
		var maxETag string
		var maxPartMeta string
		for etag, quorum := range partMetaQuorumMap {
			if maxQuorum < quorum {
				maxQuorum = quorum
				maxETag = etag
				maxPartMeta = etag
			}
		}
		// found is a representative ObjectPartInfo which either has the maximally occurring ETag or an error.
		var found *ObjectPartInfo
		for _, pinfo := range pinfos {
			if pinfo == nil {
				continue
			}
			if maxETag != "" && pinfo.ETag == maxETag {
				found = pinfo
				break
			}
			if pinfo.ETag == "" && maxPartMeta != "" && path.Base(maxPartMeta) == fmt.Sprintf("part.%d.meta", pinfo.Number) {
				found = pinfo
				break
			}
		}

		if found != nil && found.ETag != "" && partMetaQuorumMap[maxETag] >= readQuorum {
			partInfosInQuorum[pidx] = *found
			continue
		}
		partInfosInQuorum[pidx] = ObjectPartInfo{
			Number: partNumbers[pidx],
			Error: InvalidPart{
				PartNumber: partNumbers[pidx],
			}.Error(),
		}

	}
	return partInfosInQuorum, nil
}

func objPartToPartErr(part ObjectPartInfo) error {
	if strings.Contains(part.Error, "file not found") {
		return InvalidPart{PartNumber: part.Number}
	}
	if strings.Contains(part.Error, "Specified part could not be found") {
		return InvalidPart{PartNumber: part.Number}
	}
	if strings.Contains(part.Error, errErasureReadQuorum.Error()) {
		return errErasureReadQuorum
	}
	return errors.New(part.Error)
}

// CompleteMultipartUpload - completes an ongoing multipart
// transaction after receiving all the parts indicated by the client.
// Returns an md5sum calculated by concatenating all the individual
// md5sums of all the parts.
//
// Implements S3 compatible Complete multipart API.
func (er erasureObjects) CompleteMultipartUpload(ctx context.Context, bucket string, object string, uploadID string, parts []CompletePart, opts ObjectOptions) (oi ObjectInfo, err error) {
	defer multipartLatency.CompleteMultipartUploadLatency(ctx, bucket, object, uploadID)()

	if !opts.NoAuditLog {
		auditObjectErasureSet(ctx, "CompleteMultipartUpload", object, &er)
	}

	if opts.CheckPrecondFn != nil {
		if !opts.NoLock {
			ns := er.NewNSLock(bucket, object)
			lkctx, err := ns.GetLock(ctx, globalOperationTimeout)
			if err != nil {
				return ObjectInfo{}, err
			}
			ctx = lkctx.Context()
			defer ns.Unlock(lkctx)
			opts.NoLock = true
		}

		obj, err := er.getObjectInfo(ctx, bucket, object, opts)
		if err == nil && opts.CheckPrecondFn(obj) {
			return ObjectInfo{}, PreConditionFailed{}
		}
		if err != nil && !isErrVersionNotFound(err) && !isErrObjectNotFound(err) && !isErrReadQuorum(err) {
			return ObjectInfo{}, err
		}
	}

	fi, partsMetadata, activeDisks, err := er.checkUploadIDExists(ctx, bucket, object, uploadID, true)
	if err != nil {
		if errors.Is(err, errVolumeNotFound) {
			return oi, toObjectErr(err, bucket)
		}
		return oi, toObjectErr(err, bucket, object, uploadID)
	}

	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)
	// onlineDisks := er.getDisks()
	// activeDisks, _, _ := er.GetActiveInfo(ctx, er.getDisks())
	writeQuorum := fi.WriteQuorum(er.defaultWQuorum())
	readQuorum := fi.ReadQuorum(er.defaultRQuorum())

	// Read Part info for all parts
	partPath := pathJoin(uploadIDPath, fi.DataDir) + SlashSeparator
	partMetaPaths := make([]string, len(parts))
	partNumbers := make([]int, len(parts))
	for idx, part := range parts {
		partMetaPaths[idx] = pathJoin(partPath, fmt.Sprintf("part.%d.meta", part.PartNumber))
		partNumbers[idx] = part.PartNumber
	}

	partInfoFiles, err := readParts(ctx, activeDisks, minioMetaMultipartBucket, partMetaPaths, partNumbers, readQuorum)
	if err != nil {
		return oi, err
	}

	if len(partInfoFiles) != len(parts) {
		// Should only happen through internal error
		err := fmt.Errorf("unexpected part result count: %d, want %d", len(partInfoFiles), len(parts))
		bugLogIf(ctx, err)
		return oi, toObjectErr(err, bucket, object)
	}

	// Checksum type set when upload started.
	var checksumType hash.ChecksumType
	if cs := fi.Metadata[hash.MinIOMultipartChecksum]; cs != "" {
		checksumType = hash.NewChecksumType(cs)
		if opts.WantChecksum != nil && !opts.WantChecksum.Type.Is(checksumType) {
			return oi, InvalidArgument{
				Bucket: bucket,
				Object: fi.Name,
				Err:    fmt.Errorf("checksum type mismatch"),
			}
		}
	}

	var checksumCombined []byte

	// However, in case of encryption, the persisted part ETags don't match
	// what we have sent to the client during PutObjectPart. The reason is
	// that ETags are encrypted. Hence, the client will send a list of complete
	// part ETags of which may not match the ETag of any part. For example
	//   ETag (client):          30902184f4e62dd8f98f0aaff810c626
	//   ETag (server-internal): 20000f00ce5dc16e3f3b124f586ae1d88e9caa1c598415c2759bbb50e84a59f630902184f4e62dd8f98f0aaff810c626
	//
	// Therefore, we adjust all ETags sent by the client to match what is stored
	// on the backend.
	kind, _ := crypto.IsEncrypted(fi.Metadata)

	var objectEncryptionKey []byte
	switch kind {
	case crypto.SSEC:
		if checksumType.IsSet() {
			if opts.EncryptFn == nil {
				return oi, crypto.ErrMissingCustomerKey
			}
			baseKey := opts.EncryptFn("", nil)
			if len(baseKey) != 32 {
				return oi, crypto.ErrInvalidCustomerKey
			}
			objectEncryptionKey, err = decryptObjectMeta(baseKey, bucket, object, fi.Metadata)
			if err != nil {
				return oi, err
			}
		}
	case crypto.S3, crypto.S3KMS:
		objectEncryptionKey, err = decryptObjectMeta(nil, bucket, object, fi.Metadata)
		if err != nil {
			return oi, err
		}
	}
	if len(objectEncryptionKey) == 32 {
		var key crypto.ObjectKey
		copy(key[:], objectEncryptionKey)
		opts.EncryptFn = metadataEncrypter(key)
	}

	for idx, part := range partInfoFiles {
		if part.Error != "" {
			err = objPartToPartErr(part)
			bugLogIf(ctx, err)
			return oi, err
		}

		if parts[idx].PartNumber != part.Number {
			internalLogIf(ctx, fmt.Errorf("part.%d.meta has incorrect corresponding part number: expected %d, got %d", parts[idx].PartNumber, parts[idx].PartNumber, part.Number))
			return oi, InvalidPart{
				PartNumber: part.Number,
			}
		}

		// Add the current part.
		fi.AddObjectPart(part.Number, part.ETag, part.Size, part.ActualSize, part.ModTime, part.Index, part.Checksums)
	}

	// Calculate full object size.
	var objectSize int64

	// Calculate consolidated actual size.
	var objectActualSize int64

	// Order online disks in accordance with distribution order.
	// Order parts metadata in accordance with distribution order.
	activeDisks, partsMetadata = shuffleDisksAndPartsMetadataByIndex(activeDisks, partsMetadata, fi)

	// Save current erasure metadata for validation.
	currentFI := fi

	// Allocate parts similar to incoming slice.
	fi.Parts = make([]ObjectPartInfo, len(parts))

	// Validate each part and then commit to disk.
	for i, part := range parts {
		partIdx := objectPartIndex(currentFI.Parts, part.PartNumber)
		// All parts should have same part number.
		if partIdx == -1 {
			invp := InvalidPart{
				PartNumber: part.PartNumber,
				GotETag:    part.ETag,
			}
			return oi, invp
		}
		expPart := currentFI.Parts[partIdx]

		// ensure that part ETag is canonicalized to strip off extraneous quotes
		part.ETag = canonicalizeETag(part.ETag)
		expETag := tryDecryptETag(objectEncryptionKey, expPart.ETag, kind == crypto.S3)
		if expETag != part.ETag {
			invp := InvalidPart{
				PartNumber: part.PartNumber,
				ExpETag:    expETag,
				GotETag:    part.ETag,
			}
			return oi, invp
		}

		if checksumType.IsSet() {
			crc := expPart.Checksums[checksumType.String()]
			if crc == "" {
				return oi, InvalidPart{
					PartNumber: part.PartNumber,
				}
			}
			wantCS := map[string]string{
				hash.ChecksumCRC32.String():  part.ChecksumCRC32,
				hash.ChecksumCRC32C.String(): part.ChecksumCRC32C,
				hash.ChecksumSHA1.String():   part.ChecksumSHA1,
				hash.ChecksumSHA256.String(): part.ChecksumSHA256,
			}
			if wantCS[checksumType.String()] != crc {
				return oi, InvalidPart{
					PartNumber: part.PartNumber,
					ExpETag:    wantCS[checksumType.String()],
					GotETag:    crc,
				}
			}
			cs := hash.NewChecksumString(checksumType.String(), crc)
			if !cs.Valid() {
				return oi, InvalidPart{
					PartNumber: part.PartNumber,
				}
			}
			checksumCombined = append(checksumCombined, cs.Raw...)
		}

		// All parts except the last part has to be at least 5MB.
		if (i < len(parts)-1) && !isMinAllowedPartSize(currentFI.Parts[partIdx].ActualSize) {
			return oi, PartTooSmall{
				PartNumber: part.PartNumber,
				PartSize:   expPart.ActualSize,
				PartETag:   part.ETag,
			}
		}

		// Save for total object size.
		objectSize += expPart.Size

		// Save the consolidated actual size.
		objectActualSize += expPart.ActualSize

		// Add incoming parts.
		fi.Parts[i] = ObjectPartInfo{
			Number:     part.PartNumber,
			Size:       expPart.Size,
			ActualSize: expPart.ActualSize,
			ModTime:    expPart.ModTime,
			Index:      expPart.Index,
			Checksums:  nil, // Not transferred since we do not need it.
		}
	}

	if opts.WantChecksum != nil {
		err := opts.WantChecksum.Matches(checksumCombined, len(parts))
		if err != nil {
			return oi, err
		}
	}

	// Accept encrypted checksum from incoming request.
	if opts.UserDefined[ReplicationSsecChecksumHeader] != "" {
		if v, err := base64.StdEncoding.DecodeString(opts.UserDefined[ReplicationSsecChecksumHeader]); err == nil {
			fi.Checksum = v
		}
		delete(opts.UserDefined, ReplicationSsecChecksumHeader)
	}

	if checksumType.IsSet() {
		checksumType |= hash.ChecksumMultipart | hash.ChecksumIncludesMultipart
		var cs *hash.Checksum
		cs = hash.NewChecksumFromData(checksumType, checksumCombined)
		fi.Checksum = cs.AppendTo(nil, checksumCombined)
		if opts.EncryptFn != nil {
			fi.Checksum = opts.EncryptFn("object-checksum", fi.Checksum)
		}
	}
	delete(fi.Metadata, hash.MinIOMultipartChecksum) // Not needed in final object.

	// Save the final object size and modtime.
	fi.Size = objectSize
	fi.ModTime = opts.MTime
	if opts.MTime.IsZero() {
		fi.ModTime = UTCNow()
	}

	// Save successfully calculated md5sum.
	// for replica, newMultipartUpload would have already sent the replication ETag
	if fi.Metadata["etag"] == "" {
		if opts.UserDefined["etag"] != "" {
			fi.Metadata["etag"] = opts.UserDefined["etag"]
		} else { // fallback if not already calculated in handler.
			fi.Metadata["etag"] = getCompleteMultipartMD5(parts)
		}
	}

	// Save the consolidated actual size.
	if opts.ReplicationRequest {
		if v := opts.UserDefined[ReservedMetadataPrefix+"Actual-Object-Size"]; v != "" {
			fi.Metadata[ReservedMetadataPrefix+"actual-size"] = v
		}
	} else {
		fi.Metadata[ReservedMetadataPrefix+"actual-size"] = strconv.FormatInt(objectActualSize, 10)
	}

	if opts.DataMovement {
		fi.SetDataMov()
	}

	// Update all erasure metadata, make sure to not modify fields like
	// checksum which are different on each disks.
	for index := range partsMetadata {
		if partsMetadata[index].IsValid() {
			partsMetadata[index].Size = fi.Size
			partsMetadata[index].ModTime = fi.ModTime
			partsMetadata[index].Metadata = fi.Metadata
			partsMetadata[index].Parts = fi.Parts
			partsMetadata[index].Checksum = fi.Checksum
			partsMetadata[index].Versioned = opts.Versioned || opts.VersionSuspended
		}
	}

	paths := make([]string, 0, len(currentFI.Parts))
	// Remove parts that weren't present in CompleteMultipartUpload request.
	for _, curpart := range currentFI.Parts {
		paths = append(paths, pathJoin(uploadIDPath, currentFI.DataDir, fmt.Sprintf("part.%d.meta", curpart.Number)))

		if objectPartIndex(fi.Parts, curpart.Number) == -1 {
			// Delete the missing part files. e.g,
			// Request 1: NewMultipart
			// Request 2: PutObjectPart 1
			// Request 3: PutObjectPart 2
			// Request 4: CompleteMultipartUpload --part 2
			// N.B. 1st part is not present. This part should be removed from the storage.
			paths = append(paths, pathJoin(uploadIDPath, currentFI.DataDir, fmt.Sprintf("part.%d", curpart.Number)))
		}
	}

	if !opts.NoLock {
		lk := er.NewNSLock(bucket, object)
		lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
		if err != nil {
			return ObjectInfo{}, err
		}
		ctx = lkctx.Context()
		defer lk.Unlock(lkctx)
	}

	er.cleanupMultipartPath(ctx, activeDisks, paths...) // cleanup all part.N.meta, and skipped part.N's before final rename().

	defer func() {
		if err == nil {
			er.deleteAll(context.Background(), minioMetaMultipartBucket, uploadIDPath)
		}
	}()

	// Rename the multipart object to final location.
	activeDisks, versions, oldDataDir, err := renameData(ctx, activeDisks, minioMetaMultipartBucket, uploadIDPath,
		partsMetadata, bucket, object, writeQuorum)
	if err != nil {
		return oi, toObjectErr(err, bucket, object, uploadID)
	}

	if err = er.commitRenameDataDir(ctx, bucket, object, oldDataDir, activeDisks, writeQuorum); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object, uploadID)
	}

	if !opts.Speedtest && len(versions) > 0 {
		globalMRFState.addPartialOp(PartialOperation{
			Bucket:    bucket,
			Object:    object,
			Queued:    time.Now(),
			Versions:  versions,
			SetIndex:  er.setIndex,
			PoolIndex: er.poolIndex,
		})
	}

	if !opts.Speedtest && len(versions) == 0 {
		// Check if there is any offline disk and add it to the MRF list
		for _, disk := range activeDisks {
			if disk != nil && disk.IsOnline() {
				continue
			}
			er.addPartial(bucket, object, fi.VersionID)
			break
		}
	}

	for i := 0; i < len(activeDisks); i++ {
		if activeDisks[i] != nil && activeDisks[i].IsOnline() {
			// Object info is the same in all disks, so we can pick
			// the first meta from online disk
			fi = partsMetadata[i]
			break
		}
	}

	// we are adding a new version to this object under the namespace lock, so this is the latest version.
	fi.IsLatest = true

	// Success, return object info.
	return fi.ToObjectInfo(bucket, object, opts.Versioned || opts.VersionSuspended), nil
}

// AbortMultipartUpload - aborts an ongoing multipart operation
// signified by the input uploadID. This is an atomic operation
// doesn't require clients to initiate multiple such requests.
//
// All parts are purged from all disks and reference to the uploadID
// would be removed from the system, rollback is not possible on this
// operation.
func (er erasureObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) (err error) {
	if !opts.NoAuditLog {
		auditObjectErasureSet(ctx, "AbortMultipartUpload", object, &er)
	}

	// Validates if upload ID exists.
	if _, _, _, err = er.checkUploadIDExists(ctx, bucket, object, uploadID, false); err != nil {
		if errors.Is(err, errVolumeNotFound) {
			return toObjectErr(err, bucket)
		}
		return toObjectErr(err, bucket, object, uploadID)
	}

	// Cleanup all uploaded parts.
	er.deleteAll(ctx, minioMetaMultipartBucket, er.getUploadIDDir(bucket, object, uploadID))

	// Successfully purged.
	return nil
}
