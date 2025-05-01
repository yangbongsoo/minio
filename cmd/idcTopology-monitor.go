package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/minio/minio/internal/logger"
)

const idcMonitorInterval = 15 * time.Second // 모니터링 주기 15초로 설정

func init() {

	// 기본 토폴로지 파일 경로 설정
	globalIDCState.TopologyPath = "/tmp/minio/topology/idc-topology.json"
	globalIDCState.IDCInfoMap = make(map[string]*IDCInfo)
}

// startIDCTopologyMonitor는 IDC 토폴로지 모니터링 고루틴을 시작.
func startIDCTopologyMonitor(ctx context.Context) {
	logger.LogIf(ctx, "idcTopology-monitor.startIDCTopologyMonitor", fmt.Errorf("[YBS] Starting IDC topology monitor...\n"))
	go monitorIDCTopology(ctx)
}

// monitorIDCTopology는 주기적으로 IDC 토폴로지 파일을 확인하고 전역 상태를 업데이트.
func monitorIDCTopology(ctx context.Context) {
	var lastModTime time.Time

	ticker := time.NewTicker(idcMonitorInterval)
	defer ticker.Stop()

	// 초기 실행: 서버 시작 시 즉시 상태 로드 시도
	updateIDCTopology(ctx, &lastModTime)

	for {
		select {
		case <-ctx.Done():
			logger.LogIf(ctx, "idcTopology-monitor.monitorIDCTopology", fmt.Errorf("[YBS] Stopping IDC topology monitor...\n"))
			return
		case <-ticker.C:
			updateIDCTopology(ctx, &lastModTime)
		}
	}
}

func updateIDCTopologyPath(path string) {
	globalIDCState.TopologyPath = path
}

// updateIDCTopology는 IDC 토폴로지 파일을 읽고 전역 상태를 업데이트.
func updateIDCTopology(ctx context.Context, lastModTime *time.Time) {
	filePath := globalIDCState.TopologyPath
	logger.LogIf(ctx, "idcTopology-monitor.updateIDCTopology", fmt.Errorf("[YBS] updateIDCTopology start\n"))

	_ = lastModTime
	// TODO: 테스트를 위해 파일 수정 시간 확인해서 변경있을때만 업데이트 하는 로직 주석 처리
	// 파일 상태 확인
	// fileInfo, err := os.Stat(filePath)
	// if err != nil {
	// 	// 파일이 없거나 접근 오류 시 로그 기록 (오류 수준 조정 가능)
	// 	logger.LogIf(ctx, "idcTopology-monitor.updateIDCTopology", fmt.Errorf("[YBS] IDC topology file check error for %s: %w\n", filePath, err))
	// 	// 파일 접근 불가 시 기존 상태 유지 또는 초기화 결정 필요
	// 	// 여기서는 기존 상태 유지
	// 	return
	// }

	// 파일 수정 시간 확인
	// modTime := fileInfo.ModTime()
	// if modTime.Equal(*lastModTime) {
	// 	logger.LogIf(ctx, "idcTopology-monitor.updateIDCTopology", fmt.Errorf("[YBS] IDC topology file not changed\n"))
	// 	return // 변경 없음
	// }
	// *lastModTime = modTime // 마지막 수정 시간 업데이트
	// logger.LogIf(ctx, "idcTopology-monitor.updateIDCTopology", fmt.Errorf("[YBS] IDC topology file changed, updating state(modTime: %v)\n", modTime))

	// 파일 읽기
	data, err := os.ReadFile(filePath)
	if err != nil {
		logger.LogIf(ctx, "idcTopology-monitor.updateIDCTopology", fmt.Errorf("[YBS] IDC topology file read error for %s: %w\n", filePath, err))
		return
	}

	logger.LogIf(ctx, "idcTopology-monitor.updateIDCTopology", fmt.Errorf("[YBS] IDC topology file read successfully\n"))

	// JSON 데이터 파싱 (임시 구조체 사용)
	var idcNodeInfoMap map[string][]IDCNodeInfo
	if err := json.Unmarshal(data, &idcNodeInfoMap); err != nil {
		logger.LogIf(ctx, "idcTopology-monitor.updateIDCTopology", fmt.Errorf("[YBS] IDC topology JSON parse error for %s: %w\n", filePath, err))
		return
	}

	logger.LogIf(ctx, "idcTopology-monitor.updateIDCTopology", fmt.Errorf("[YBS] Raw Topology Data: %#v", idcNodeInfoMap))

	// 전역 상태 업데이트
	globalIDCState.Lock()
	defer globalIDCState.Unlock()

	newIDCInfoMap := make(map[string]*IDCInfo)
	// 각 IDC 상태 분석 및 업데이트
	for idcName, idcNodeInfos := range idcNodeInfoMap {
		totalNodeCount := len(idcNodeInfos)
		notReadyNodeCount := 0
		for _, node := range idcNodeInfos {
			if node.NodeStatus != "Ready" {
				notReadyNodeCount++
			}
		}

		// IDC 활성 상태 결정 (75% 이상 NotReady이면 비활성)
		// 주의: totalNodes가 0일 경우 나누기 오류 방지
		isActive := true
		if totalNodeCount > 0 {
			failureThreshold := int(float64(totalNodeCount) * 0.75)
			// NotReady 노드 수가 임계치 이상이면 비활성
			if notReadyNodeCount >= failureThreshold {
				isActive = false
			}
		} else {
			isActive = false // 노드가 없으면 비활성으로 간주
		}

		newIDCInfoMap[idcName] = &IDCInfo{
			IDCNodeInfos:      idcNodeInfos, // 원본 슬라이스 참조 (필요시 복사)
			TotalNodeCount:    totalNodeCount,
			NotReadyNodeCount: notReadyNodeCount,
			IsActive:          isActive,
		}
		logger.LogIf(ctx, "idcTopology-monitor.updateIDCTopology", fmt.Errorf("[YBS] Updated IDC '%s': Total=%d, NotReady=%d, Active=%t\n", idcName, totalNodeCount, notReadyNodeCount, isActive))
	}

	globalIDCState.IDCInfoMap = newIDCInfoMap // 새로운 상태로 교체
	globalIDCState.LastCheckTime = UTCNow()   // 마지막 확인 시간 업데이트

	logger.LogIf(ctx, "idcTopology-monitor.updateIDCTopology", fmt.Errorf("[YBS] IDC topology state updated successfully.\n"))
}

// GetActiveIDCCount는 현재 활성 상태인 IDC 수를 반환.
func GetActiveIDCCount() int {
	globalIDCState.RLock()
	defer globalIDCState.RUnlock()

	activeCount := 0
	for _, idcInfo := range globalIDCState.IDCInfoMap {
		if idcInfo.IsActive {
			activeCount++
		}
	}
	return activeCount
}
