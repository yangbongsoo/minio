# MinIO 시스템 구조 다이어그램 (IDC-aware EC & Multipart)

```mermaid
flowchart TB
    subgraph "IDC 동적 감지 및 상태 반영"
        idcMonitor[idcTopology-monitor.go\nIDC 토폴로지 파일 모니터링 및\nIDC 상태 동적 감지]
        globalIDCState[globalIDCState\n전역 IDC 상태 정보]
    end

    subgraph "Erasure 서버 및 디스크 초기화"
        newErasureServerPools[newErasureServerPools\n서버 풀/디스크 초기화]
        newErasureSets[newErasureSets\nErasure 세트 구성]
        xlStorage[xlStorage\n디스크 접근]
    end

    subgraph "객체 작업 처리 (erasure-object.go)"
        PutObject[PutObject\n단일 객체 업로드]
        putObject[putObject\n실제 EC 파라미터/activeDisks 결정]
        PutObjectIDC[putObjectIDC\nIDC-aware EC 적용]
    end

    subgraph "Multipart Upload (erasure-multipart.go)"
        NewMultipartUpload[NewMultipartUpload\n멀티파트 업로드 시작]
        newMultipartUploadIDC[newMultipartUploadIDC\nIDC-aware EC/activeDisks 결정\n메타데이터에 기록]
        PutObjectPart[PutObjectPart\n멀티파트 파트 업로드]
        putObjectPartIDC[putObjectPartIDC\n메타데이터 기반 activeDisks만 사용]
        CompleteMultipartUpload[CompleteMultipartUpload\n멀티파트 업로드 완료]
    end

    subgraph "메타데이터 및 분포 관리"
        FileInfo[FileInfo\n메타데이터 구조체\n(active_disks, 분포 등 포함)]
        Metadata[Metadata (userDefined)\nactive_disks, EC 파라미터, 분포 기록]
    end

    %% IDC 감지 및 EC 파라미터 결정 흐름
    idcMonitor --> globalIDCState
    globalIDCState -->|IDC 상태/노드 헬스| putObject
    globalIDCState -->|IDC 상태/노드 헬스| newMultipartUploadIDC

    %% 단일 객체 업로드 흐름
    PutObject --> putObject
    putObject --> PutObjectIDC
    PutObjectIDC --> FileInfo
    FileInfo --> Metadata
    Metadata -->|active_disks, 분포 기록| FileInfo

    %% Multipart 업로드 흐름
    NewMultipartUpload --> newMultipartUploadIDC
    newMultipartUploadIDC -->|업로드 시작 시점의 activeDisks/분포/EC 파라미터 결정 및 메타데이터 기록| FileInfo
    FileInfo --> Metadata
    Metadata -->|active_disks, 분포 기록| FileInfo
    newMultipartUploadIDC --> PutObjectPart
    PutObjectPart --> putObjectPartIDC
    putObjectPartIDC -->|메타데이터에서 activeDisks/분포 복원| FileInfo
    CompleteMultipartUpload -->|메타데이터에서 activeDisks/분포 복원| FileInfo

    %% 디스크/세트 초기화
    newErasureServerPools --> newErasureSets
    newErasureSets --> xlStorage

    %% 기타
    PutObjectIDC --> xlStorage
    putObjectPartIDC --> xlStorage
    CompleteMultipartUpload --> xlStorage

    %% 구조 강조
    classDef component fill:#f9f,stroke:#333,stroke-width:2px;
    classDef storage fill:#bbf,stroke:#333,stroke-width:2px;
    classDef monitor fill:#bfb,stroke:#333,stroke-width:2px;
    class idcMonitor,globalIDCState monitor;
    class newErasureServerPools,newErasureSets,xlStorage storage;
    class PutObject,putObject,PutObjectIDC,NewMultipartUpload,newMultipartUploadIDC,PutObjectPart,putObjectPartIDC,CompleteMultipartUpload,FileInfo,Metadata component;
```

## 주요 흐름 및 메커니즘 설명

### 1. IDC 동적 감지 및 EC 파라미터 결정
- **idcTopology-monitor.go**가 IDC 토폴로지 파일을 모니터링하여 IDC 상태를 동적으로 감지
- **globalIDCState**에 현재 IDC/노드 헬스 정보를 갱신
- 객체 업로드/멀티파트 시작 시점에 이 정보를 참조하여 활성 IDC/노드만 activeDisks로 선정, EC 파라미터(분포) 결정

### 2. 단일 객체 업로드 (PutObject)
- **putObject**에서 IDC 상태에 따라 EC 파라미터/activeDisks 결정
- **PutObjectIDC**에서 실제 데이터/파라미터/분포를 메타데이터(FileInfo)에 기록

### 3. Multipart Upload
- **newMultipartUploadIDC**에서 업로드 시작 시점의 activeDisks(순서/구성), EC 파라미터, 분포를 메타데이터(userDefined["active_disks"])에 기록
- 이후 **PutObjectPartIDC**, **CompleteMultipartUpload** 등 모든 단계에서 FileInfo의 메타데이터에 기록된 activeDisks/분포만 사용 (동적으로 다시 계산하지 않음)
- 하나라도 불일치/누락 시 즉시 에러 반환

### 4. 메타데이터 기반 분포/activeDisks 일치
- FileInfo.Metadata에 기록된 active_disks, 분포(Distribution), EC 파라미터를 기준으로 모든 작업이 일관되게 동작
- 장애/IDC 변화 시 기존 업로드는 중단, 새 업로드는 새로운 환경으로 시작

### 5. 디스크/세트 초기화
- 서버 시작 시 디스크/세트/스토리지 초기화 및 상태 점검

---

이 구조는 **IDC-aware EC, 동적 IDC 감지, 메타데이터 기반 분포/activeDisks 일치**를 모두 반영합니다.
